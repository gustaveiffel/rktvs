// SPDX-License-Identifier: AGPL-3.0-only

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};

use rk_core::catalog::{Catalog, LibraryRecord};
use rk_core::chunk_store::ChunkStore;
use rk_core::indexer::IndexConfig;
use rk_core::manifest::Manifest;
use rk_core::resolver::ChunkResolver;
use rk_core::verifier;
use rk_tar::export;
use rk_tar::ingest::{self, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};
use rk_transport::cert;

#[derive(Parser)]
#[command(
    name = "rk",
    about = "Content-addressed file delivery over hostile networks"
)]
struct Cli {
    /// Data directory for chunk store and catalog
    #[arg(long, env = "RK_DATA_DIR", default_value = "~/.rk")]
    data_dir: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Ingest a tar archive from stdin
    Ingest {
        /// Tape name to ingest into
        #[arg(long, default_value = "default")]
        tape: String,
    },
    /// Export files as a tar archive to stdout
    Tar {
        /// Path in format <tape>/ or <tape>/<prefix>
        path: String,
    },
    /// List files in the catalog
    Ls {
        /// Path in format <tape>/ or <tape>/<prefix>
        path: String,
    },
    /// Estimate transfer cost for a file (no data transfer)
    Estimate {
        /// Path in format <tape>/<path>
        path: String,
    },
    /// List transfer jobs
    Jobs {
        /// Filter by status (pending, running, completed, cancelled)
        #[arg(long)]
        status: Option<String>,
    },
    /// Index files in-place (zero-copy by-reference chunking)
    Index {
        /// Tape name to index into
        #[arg(long, default_value = "default")]
        tape: String,
        /// Directory path to index
        path: String,
    },
    /// Verify integrity of indexed files
    Verify {
        /// Tape to verify (verifies all if omitted)
        #[arg(long)]
        tape: Option<String>,
        /// Full BLAKE3 hash verification (slower)
        #[arg(long)]
        blake3: bool,
    },
    /// Hub server commands
    Hub {
        #[command(subcommand)]
        command: HubCommands,
    },
    /// Manage remote libraries
    Library {
        #[command(subcommand)]
        command: LibraryCommands,
    },
    /// Fetch a file from a remote library
    Fetch {
        /// Path in format <library>:<tape>/<path>
        path: String,
        /// Transfer priority grade
        #[arg(long, default_value = "normal")]
        grade: String,
    },
}

#[derive(Subcommand)]
enum HubCommands {
    /// Initialize a hub data directory (generates TLS cert)
    Init {
        /// Listen address (used for certificate SANs)
        #[arg(long, default_value = "0.0.0.0:4443")]
        listen: String,
        /// Additional Subject Alternative Names (IPs or hostnames)
        #[arg(long = "san")]
        sans: Vec<String>,
    },
    /// Start the hub server
    Serve {
        /// Listen address
        #[arg(long, default_value = "0.0.0.0:4443")]
        listen: String,
    },
}

#[derive(Subcommand)]
enum LibraryCommands {
    /// Register a remote library
    Add {
        /// Library identifier
        id: String,
        /// Endpoint address (host:port)
        endpoint: String,
        /// Path to the hub's certificate (.der file)
        #[arg(long)]
        cert: String,
        /// Replace existing library (removes old entry first)
        #[arg(long)]
        force: bool,
    },
    /// List known libraries
    List,
    /// Remove a library
    Remove {
        /// Library identifier
        id: String,
    },
    /// Ping a library to check connectivity
    Ping {
        /// Library identifier
        id: String,
    },
    /// Sync catalog metadata from a remote library
    Sync {
        /// Library identifier
        id: String,
        /// Sync only this tape (syncs all if omitted)
        #[arg(long)]
        tape: Option<String>,
    },
}

fn resolve_data_dir(raw: &str) -> PathBuf {
    if let Some(rest) = raw.strip_prefix("~/")
        && let Some(home) = dirs_next::home_dir()
    {
        return home.join(rest);
    }
    PathBuf::from(raw)
}

/// Parse "<tape>/<path>" into (tape, path_prefix).
fn parse_tape_path(input: &str) -> Result<(&str, &str)> {
    match input.split_once('/') {
        Some((tape, rest)) => {
            if tape.is_empty() {
                bail!("tape name cannot be empty in '{}'", input);
            }
            if rest.is_empty() {
                Ok((tape, "/"))
            } else {
                Ok((tape, rest))
            }
        }
        None => bail!("expected format <tape>/<path>, got '{}'", input),
    }
}

/// Validate a library ID: alphanumeric, hyphens, underscores only.
/// Prevents path traversal when the ID is used in filesystem paths.
fn validate_library_id(id: &str) -> Result<()> {
    if id.is_empty() {
        bail!("library ID cannot be empty");
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        bail!(
            "invalid library ID '{id}': only alphanumeric characters, hyphens, \
             and underscores are allowed"
        );
    }
    Ok(())
}

/// Parse "<library>:<tape>/<path>" into (library, tape, file_path).
fn parse_library_tape_path(input: &str) -> Result<(&str, &str, &str)> {
    let (library, rest) = input.split_once(':').ok_or_else(|| {
        anyhow::anyhow!("expected format <library>:<tape>/<path>, got '{}'", input)
    })?;
    if library.is_empty() {
        bail!("library name cannot be empty in '{}'", input);
    }
    let (tape, path) = parse_tape_path(rest)?;
    Ok((library, tape, path))
}

/// Simple glob matching for file paths.
/// Supports `*` (any chars except `/`) and `?` (any single char except `/`).
/// Pattern is matched against the full path (not just the filename).
fn glob_match(pattern: &str, path: &str) -> bool {
    fn do_match(pat: &[u8], text: &[u8]) -> bool {
        let (mut pi, mut ti) = (0, 0);
        let (mut star_pi, mut star_ti) = (usize::MAX, 0);

        while ti < text.len() {
            if pi < pat.len() && pat[pi] == b'?' && text[ti] != b'/' {
                pi += 1;
                ti += 1;
            } else if pi < pat.len() && pat[pi] == b'*' {
                star_pi = pi;
                star_ti = ti;
                pi += 1;
            } else if pi < pat.len() && pat[pi] == text[ti] {
                pi += 1;
                ti += 1;
            } else if star_pi != usize::MAX {
                // Backtrack: * should not match /
                star_ti += 1;
                if text[star_ti - 1] == b'/' {
                    return false;
                }
                ti = star_ti;
                pi = star_pi + 1;
            } else {
                return false;
            }
        }
        // Skip trailing stars
        while pi < pat.len() && pat[pi] == b'*' {
            pi += 1;
        }
        pi == pat.len()
    }

    // Pattern from CLI doesn't have leading /, but catalog paths do
    let normalized = if !pattern.starts_with('/') && path.starts_with('/') {
        format!("/{pattern}")
    } else {
        pattern.to_string()
    };

    do_match(normalized.as_bytes(), path.as_bytes())
}

/// Resolve a relative cert path against the data directory.
fn resolve_cert_path(data_dir: &Path, rel: &str) -> PathBuf {
    data_dir.join(rel)
}

/// Extract server_name for TLS SNI from an endpoint string.
/// For IP-based endpoints, returns the IP as a string.
fn extract_hostname(endpoint: &str) -> Result<String> {
    let addr: SocketAddr = endpoint
        .parse()
        .with_context(|| format!("invalid endpoint: {endpoint}"))?;
    Ok(addr.ip().to_string())
}

/// Connect to a library: load cert, build TLS config, establish QUIC connection.
async fn connect_to_library(
    catalog: &Catalog,
    data_dir: &Path,
    library_id: &str,
    satellite_id: &str,
) -> Result<(rk_transport::satellite::Satellite, LibraryRecord)> {
    let lib = catalog
        .get_library(library_id)?
        .ok_or_else(|| anyhow::anyhow!("unknown library '{library_id}'"))?;
    let cert_rel = lib
        .cert_path
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("no cert path for library '{library_id}'"))?;
    let cert_abs = resolve_cert_path(data_dir, cert_rel);

    let hub_cert =
        cert::load_cert(&cert_abs).with_context(|| format!("loading cert for '{library_id}'"))?;
    let client_config = cert::client_config(&hub_cert).context("building TLS client config")?;

    let addr: SocketAddr = lib
        .endpoint
        .parse()
        .with_context(|| format!("invalid endpoint: {}", lib.endpoint))?;
    let server_name = extract_hostname(&lib.endpoint)?;

    let satellite = tokio::time::timeout(
        Duration::from_secs(10),
        rk_transport::satellite::Satellite::connect(
            addr,
            &server_name,
            client_config,
            satellite_id,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("connection timed out after 10s"))?
    .context("connecting to hub")?;

    Ok((satellite, lib))
}

#[tokio::main]
async fn main() -> Result<()> {
    // User-friendly panic messages instead of raw stack traces
    std::panic::set_hook(Box::new(|info| {
        let payload = info
            .payload()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| info.payload().downcast_ref::<String>().map(|s| s.as_str()))
            .unwrap_or("unknown error");
        let location = info
            .location()
            .map(|l| format!(" at {}:{}", l.file(), l.line()))
            .unwrap_or_default();
        eprintln!("rk: internal error: {payload}{location}");
        eprintln!(
            "this is a bug — please report it at https://github.com/gustaveiffel/rktvs/issues"
        );
    }));

    tracing_subscriber::fmt::init();

    let version: &str = Box::leak(
        format!(
            "{} (protocol v{})",
            env!("CARGO_PKG_VERSION"),
            rk_transport::satellite::PROTOCOL_VERSION,
        )
        .into_boxed_str(),
    );
    let cli = Cli::from_arg_matches(&Cli::command().version(version).get_matches())
        .context("parsing arguments")?;
    let data_dir = resolve_data_dir(&cli.data_dir);
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("creating data directory: {}", data_dir.display()))?;

    let store = ChunkStore::new(data_dir.clone());
    let catalog =
        Catalog::open(data_dir.join("catalog.db").as_path()).context("opening catalog")?;

    // Load manifest if it exists
    let manifest_path = data_dir.join("manifest.rkm");
    let mut manifest = if manifest_path.exists() {
        Some(Manifest::read_from_file(&manifest_path).context("reading manifest")?)
    } else {
        None
    };

    match cli.command {
        Commands::Ingest { tape } => {
            let stdin = io::stdin();
            let stats = ingest::ingest_tar(
                stdin.lock(),
                &store,
                &catalog,
                "local",
                &tape,
                MIN_CHUNK_SIZE,
                AVG_CHUNK_SIZE,
                MAX_CHUNK_SIZE,
            )
            .context("ingesting tar")?;
            eprintln!(
                "ingested {} files, {} dirs, {} bytes, {} chunks",
                stats.files, stats.dirs, stats.bytes, stats.chunks
            );
        }
        Commands::Tar { path } => {
            let resolver = ChunkResolver::new(manifest.as_ref(), &store);
            let (tape, prefix) = parse_tape_path(&path)?;
            let stdout = io::stdout();
            export::export_tar(stdout.lock(), &resolver, &catalog, "local", tape, prefix)
                .context("exporting tar")?;
        }
        Commands::Ls { path } => {
            // Support both "tape/path" (local) and "library:tape/path" (remote)
            let (library_id, tape, prefix) = if path.contains(':') {
                parse_library_tape_path(&path)?
            } else {
                let (tape, prefix) = parse_tape_path(&path)?;
                ("local", tape, prefix)
            };
            let files = catalog
                .list_files(library_id, tape, prefix)
                .context("listing files")?;
            if files.is_empty() {
                if library_id != "local" {
                    eprintln!(
                        "no files found for {path}\n\
                         hint: run `rk library sync {library_id}` to fetch catalog metadata first"
                    );
                } else {
                    eprintln!("no files found in {path}");
                }
            } else {
                for f in &files {
                    let kind = if f.entry_type == 2 { "d" } else { "-" };
                    let mode = f.mode.unwrap_or(0);
                    println!("{}{:03o}  {:>10}  {}", kind, mode, f.size, f.path);
                }
            }
        }
        Commands::Estimate { path } => {
            let resolver = ChunkResolver::new(manifest.as_ref(), &store);
            let (tape, file_path) = parse_tape_path(&path)?;
            let est = rk_scheduler::estimate::estimate_file(
                &catalog, &resolver, "local", tape, file_path,
            )?;
            eprintln!("File: {}", path);
            eprintln!("  Total chunks:   {}", est.total_chunks);
            eprintln!("  Local chunks:   {}", est.local_chunks);
            eprintln!("  Missing chunks: {}", est.missing_chunks);
            eprintln!("  Total size:     {} bytes", est.total_bytes);
            eprintln!("  Transfer est:   {} bytes", est.transfer_bytes);
            eprintln!("  Wire transfer:  ~{} bytes (compressed)", est.compressed_transfer_bytes);
            if est.compressed_transfer_bytes < est.transfer_bytes && est.transfer_bytes > 0 {
                let savings = 100 - (est.compressed_transfer_bytes * 100 / est.transfer_bytes);
                eprintln!("  Compression:    ~{}% savings", savings);
            }
        }
        Commands::Jobs { status } => {
            let jobs = catalog.list_jobs(status.as_deref())?;
            if jobs.is_empty() {
                eprintln!("no jobs");
            } else {
                for j in &jobs {
                    let progress = match j.total_chunks {
                        Some(total) if total > 0 => format!("{}/{}", j.completed_chunks, total),
                        _ => format!("{}", j.completed_chunks),
                    };
                    println!(
                        "{} {} {} {} [{}] {}",
                        j.job_id,
                        j.status,
                        j.job_type,
                        j.file_path.as_deref().unwrap_or("-"),
                        progress,
                        rk_scheduler::types::Grade::from_i32(j.grade)
                            .map(|g| g.as_str())
                            .unwrap_or("?"),
                    );
                }
            }
        }
        Commands::Index { tape, path } => {
            let dir = PathBuf::from(&path);
            if !dir.is_dir() {
                bail!("not a directory: {}", path);
            }
            let mut m = manifest.take().unwrap_or_default();
            let config = IndexConfig::default();
            let stats =
                rk_core::indexer::index_directory(&dir, &mut m, &catalog, "local", &tape, &config)
                    .context("indexing directory")?;
            m.write_to_file(&manifest_path)
                .context("writing manifest")?;
            eprintln!(
                "indexed {} files, {} dirs, {} chunks ({} new, {} dedup), {} bytes",
                stats.files_indexed,
                stats.dirs_found,
                stats.chunks_total,
                stats.chunks_new,
                stats.chunks_dedup,
                stats.total_bytes
            );
        }
        Commands::Verify { tape: _, blake3 } => {
            let m = manifest.as_ref().ok_or_else(|| {
                anyhow::anyhow!("no manifest found at {}", manifest_path.display())
            })?;
            let result = verifier::verify_manifest(m, blake3);
            eprintln!("Checked:   {}", result.chunks_checked);
            eprintln!("OK:        {}", result.chunks_ok);
            eprintln!("Stale:     {}", result.chunks_stale);
            eprintln!("Missing:   {}", result.chunks_missing);
            eprintln!("Corrupted: {}", result.chunks_corrupted);
            if result.chunks_stale > 0 || result.chunks_missing > 0 || result.chunks_corrupted > 0 {
                bail!(
                    "verification failed: {} stale, {} missing, {} corrupted",
                    result.chunks_stale,
                    result.chunks_missing,
                    result.chunks_corrupted
                );
            }
        }

        // ── Hub commands ────────────────────────────────────
        Commands::Hub { command } => match command {
            HubCommands::Init {
                listen,
                sans: extra_sans,
            } => {
                let cert_path = data_dir.join("hub.cert.der");
                let key_path = data_dir.join("hub.key.der");

                if cert_path.exists() {
                    bail!(
                        "hub already initialized (cert exists at {}). \
                         Remove it to re-initialize.",
                        cert_path.display()
                    );
                }

                // Build SANs: always include localhost + 127.0.0.1,
                // plus the listen address host if it's not a wildcard,
                // plus any extra SANs from --san flags.
                let mut sans = vec!["localhost".to_string(), "127.0.0.1".to_string()];
                if let Some(host) = listen.split(':').next()
                    && host != "0.0.0.0"
                    && host != "::"
                    && !sans.contains(&host.to_string())
                {
                    sans.push(host.to_string());
                }
                for san in &extra_sans {
                    if !sans.contains(san) {
                        sans.push(san.clone());
                    }
                }

                let (hub_cert, hub_key) = cert::generate_self_signed_for(sans.clone())
                    .context("generating self-signed certificate")?;

                cert::save_cert(&hub_cert, &cert_path).context("saving certificate")?;
                cert::save_key(&hub_key, &key_path).context("saving private key")?;

                let fingerprint = blake3::hash(hub_cert.as_ref());
                eprintln!("hub initialized");
                eprintln!("  cert:        {}", cert_path.display());
                eprintln!("  key:         {}", key_path.display());
                eprintln!("  fingerprint: {}", fingerprint.to_hex());
                eprintln!("  sans:        {}", sans.join(", "));
                eprintln!("  listen:      {}", listen);
                eprintln!();
                eprintln!("share {} with satellites to connect", cert_path.display());
            }

            HubCommands::Serve { listen } => {
                let cert_path = data_dir.join("hub.cert.der");
                let key_path = data_dir.join("hub.key.der");

                if !cert_path.exists() {
                    bail!(
                        "no certificate found at {}. Run `rk hub init` first.",
                        cert_path.display()
                    );
                }

                let hub_cert = cert::load_cert(&cert_path).context("loading certificate")?;
                let hub_key = cert::load_key(&key_path).context("loading private key")?;
                let server_config =
                    cert::server_config(hub_cert, hub_key).context("building TLS config")?;

                let addr: SocketAddr = listen
                    .parse()
                    .with_context(|| format!("invalid listen address: {listen}"))?;

                let store = Arc::new(store);
                let manifest = manifest.map(Arc::new);

                // Open a second Catalog handle for the hub (SQLite WAL allows concurrent readers)
                let hub_catalog = Arc::new(std::sync::Mutex::new(
                    Catalog::open(data_dir.join("catalog.db").as_path())
                        .context("opening hub catalog")?,
                ));

                let hub = rk_transport::hub::Hub::bind(
                    addr,
                    server_config,
                    store,
                    manifest,
                    Some(hub_catalog),
                )
                .await
                .context("binding hub server")?;

                eprintln!(
                    "rk hub v{} listening on {}",
                    env!("CARGO_PKG_VERSION"),
                    hub.local_addr()
                );
                hub.run().await;
            }
        },

        // ── Library commands ────────────────────────────────
        Commands::Library { command } => {
            match command {
                LibraryCommands::Add {
                    id,
                    endpoint,
                    cert: cert_file,
                    force,
                } => {
                    validate_library_id(&id)?;

                    // Validate endpoint format early
                    let _: SocketAddr = endpoint.parse()
                    .with_context(|| format!("invalid endpoint '{endpoint}': expected host:port (e.g. 127.0.0.1:4443)"))?;

                    let src = PathBuf::from(&cert_file);
                    if !src.exists() {
                        bail!("certificate file not found: {}", cert_file);
                    }

                    // Check for existing library before touching anything
                    let exists = catalog.get_library(&id)?.is_some();
                    if exists && !force {
                        bail!("library '{id}' already exists. Use --force to replace it.");
                    }
                    if exists {
                        catalog.remove_library(&id)?;
                        let old_cert = data_dir.join("certs").join(format!("{id}.cert.der"));
                        if old_cert.exists() {
                            std::fs::remove_file(&old_cert)?;
                        }
                    }

                    // Copy cert into data-dir/certs/<id>.cert.der
                    let certs_dir = data_dir.join("certs");
                    std::fs::create_dir_all(&certs_dir)?;
                    let dest = certs_dir.join(format!("{id}.cert.der"));
                    std::fs::copy(&src, &dest)
                        .with_context(|| format!("copying cert to {}", dest.display()))?;

                    // Store relative cert path (portable across data-dir moves)
                    let rel_cert = format!("certs/{id}.cert.der");
                    catalog
                        .add_library(&id, &id, &endpoint, &rel_cert)
                        .context("adding library")?;

                    if exists {
                        eprintln!("replaced library '{id}' at {endpoint}");
                    } else {
                        eprintln!("added library '{id}' at {endpoint}");
                    }
                    eprintln!("  cert: {rel_cert}");
                }

                LibraryCommands::List => {
                    let libs = catalog.list_libraries()?;
                    if libs.is_empty() {
                        eprintln!("no libraries registered");
                    } else {
                        for lib in &libs {
                            let seen = lib
                                .last_seen
                                .map(|ts| ts.to_string())
                                .unwrap_or_else(|| "never".into());
                            println!(
                                "{:<16} {:<24} {:<10} last_seen={}",
                                lib.library_id, lib.endpoint, lib.status, seen
                            );
                        }
                    }
                }

                LibraryCommands::Remove { id } => {
                    validate_library_id(&id)?;
                    catalog.remove_library(&id)?;
                    // Remove stored cert if present
                    let cert_file = data_dir.join("certs").join(format!("{id}.cert.der"));
                    if cert_file.exists() {
                        std::fs::remove_file(&cert_file)?;
                    }
                    eprintln!("removed library '{id}'");
                }

                LibraryCommands::Ping { id } => {
                    let start = Instant::now();
                    match connect_to_library(&catalog, &data_dir, &id, "rk-ping").await {
                        Ok((_sat, _lib)) => {
                            let elapsed = start.elapsed();
                            catalog.update_library_status(&id, "online")?;
                            eprintln!("ok ({:.1}ms)", elapsed.as_secs_f64() * 1000.0);
                        }
                        Err(e) => {
                            catalog.update_library_status(&id, "offline")?;
                            bail!("ping failed: {e:#}");
                        }
                    }
                }

                LibraryCommands::Sync { id, tape } => {
                    validate_library_id(&id)?;
                    let start = Instant::now();

                    let (satellite, _lib) = connect_to_library(&catalog, &data_dir, &id, "rk-sync")
                        .await
                        .context("connecting for catalog sync")?;

                    // Determine which tapes to sync
                    let tapes_to_sync: Vec<String> = if let Some(ref t) = tape {
                        vec![t.clone()]
                    } else {
                        // List tapes first
                        let (tape_list, _) =
                            satellite.sync_catalog("").await.context("listing tapes")?;
                        if tape_list.is_empty() {
                            eprintln!("no tapes found on '{id}'");
                            return Ok(());
                        }
                        for (name, files, size) in &tape_list {
                            eprintln!("  {name}: {files} files, {size} bytes");
                        }
                        tape_list.into_iter().map(|(name, _, _)| name).collect()
                    };

                    let mut total_files = 0u64;
                    let mut total_chunks = 0u64;

                    for tape_name in &tapes_to_sync {
                        eprintln!("syncing {id}:{tape_name}/ ...");
                        let (_, files) = satellite
                            .sync_catalog(tape_name)
                            .await
                            .with_context(|| format!("syncing tape '{tape_name}'"))?;

                        for file in &files {
                            let chunks: Vec<rk_core::chunker::ChunkMeta> = file
                                .chunks
                                .iter()
                                .map(|c| rk_core::chunker::ChunkMeta {
                                    hash: c.hash,
                                    offset: c.offset,
                                    size: c.size as usize,
                                    compressed_size: 0,
                                })
                                .collect();

                            catalog
                                .record_file(
                                    &id,
                                    tape_name,
                                    &file.path,
                                    file.entry_type,
                                    file.size,
                                    if file.mtime > 0 {
                                        Some(file.mtime)
                                    } else {
                                        None
                                    },
                                    if file.mode > 0 { Some(file.mode) } else { None },
                                    file.version,
                                    &chunks,
                                )
                                .with_context(|| format!("recording file '{}'", file.path))?;

                            total_chunks += chunks.len() as u64;
                        }
                        total_files += files.len() as u64;
                    }

                    catalog.update_library_status(&id, "online")?;
                    let elapsed = start.elapsed();
                    eprintln!(
                        "synced {} tapes, {} files, {} chunks ({:.1}ms)",
                        tapes_to_sync.len(),
                        total_files,
                        total_chunks,
                        elapsed.as_secs_f64() * 1000.0,
                    );
                }
            }
        }

        // ── Fetch command ───────────────────────────────────
        Commands::Fetch { path, grade } => {
            let (library_id, tape, file_path) = parse_library_tape_path(&path)?;

            // Parse grade
            let grade_val = match grade.to_lowercase().as_str() {
                "urgent" | "p0" => 0,
                "normal" | "p1" => 1,
                "batch" | "p2" => 2,
                "background" | "p3" => 3,
                _ => bail!("unknown grade '{grade}'. Use: urgent, normal, batch, background"),
            };

            // Resolve file list — expand globs if the path contains wildcards
            let file_paths: Vec<String> = if file_path.contains('*') || file_path.contains('?') {
                // Extract the directory prefix before the first wildcard
                let prefix = match file_path.rfind('/') {
                    Some(pos) if pos < file_path.find(['*', '?']).unwrap_or(file_path.len()) => {
                        &file_path[..=pos]
                    }
                    _ => "/",
                };
                let files = catalog.list_files(library_id, tape, prefix)?;
                if files.is_empty() {
                    bail!(
                        "no files found matching {library_id}:{tape}/{file_path}\n\
                         hint: run `rk library sync {library_id}` to fetch catalog metadata first"
                    );
                }
                let matched: Vec<String> = files
                    .into_iter()
                    .filter(|f| f.entry_type == 1 && glob_match(file_path, &f.path))
                    .map(|f| f.path)
                    .collect();
                if matched.is_empty() {
                    bail!("no files match pattern '{file_path}' in {library_id}:{tape}/");
                }
                eprintln!("{} files match pattern", matched.len());
                matched
            } else {
                vec![file_path.to_string()]
            };

            // Connect once, fetch all matching files
            let (satellite, _lib) =
                connect_to_library(&catalog, &data_dir, library_id, "rk-fetch").await?;

            let mut total_fetched = 0u64;
            let mut total_skipped = 0u64;
            let mut total_bytes_transferred = 0u64;

            for fp in &file_paths {
                let chunks = catalog.get_file_chunks(library_id, tape, fp)?;
                if chunks.is_empty() {
                    eprintln!("skipping {fp} (no chunk metadata)");
                    continue;
                }

                let fetch_path = format!("{library_id}:{tape}/{fp}");
                let job_id = format!(
                    "fetch-{}",
                    &blake3::hash(fetch_path.as_bytes()).to_hex()[..12]
                );
                let total_bytes: u64 = chunks.iter().map(|c| c.size as u64).sum();

                if let Some(existing) = catalog.get_job(&job_id)? {
                    if existing.status == "completed" {
                        eprintln!("skipping {fp} (already fetched)");
                        total_skipped += chunks.len() as u64;
                        continue;
                    }
                    eprintln!("resuming {fp} (was {})", existing.status);
                } else {
                    catalog.create_job(
                        &job_id,
                        library_id,
                        tape,
                        "fetch",
                        grade_val,
                        Some(fp.as_str()),
                        Some(chunks.len() as i64),
                        Some(total_bytes as i64),
                    )?;
                }

                eprintln!(
                    "fetching {fp} ({} chunks, {} bytes)",
                    chunks.len(),
                    total_bytes
                );
                catalog.update_job_progress(&job_id, 0, "running")?;

                let mut file_ok = true;
                for (i, chunk) in chunks.iter().enumerate() {
                    if store.has(&chunk.hash) {
                        total_skipped += 1;
                        catalog.update_job_progress(&job_id, (i + 1) as i64, "running")?;
                        continue;
                    }

                    let resp = tokio::time::timeout(
                        Duration::from_secs(30),
                        satellite.fetch_chunk(&chunk.hash),
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("chunk fetch timed out after 30s"))?
                    .context("fetching chunk")?;

                    match resp {
                        Some(resp) => {
                            total_bytes_transferred += resp.data.len() as u64;
                            if resp.compressed {
                                store
                                    .put_compressed(&chunk.hash, &resp.data)
                                    .context("storing compressed chunk")?;
                            } else {
                                let stored_hash =
                                    store.put(&resp.data).context("storing fetched chunk")?;
                                if stored_hash != chunk.hash {
                                    catalog.update_job_progress(
                                        &job_id,
                                        (i + 1) as i64,
                                        "error",
                                    )?;
                                    eprintln!(
                                        "error: hash mismatch for chunk {} in {fp}: expected {}, got {}",
                                        i,
                                        chunk.hash.to_hex(),
                                        stored_hash.to_hex()
                                    );
                                    file_ok = false;
                                    break;
                                }
                            }
                            total_fetched += 1;
                        }
                        None => {
                            catalog.update_job_progress(&job_id, (i + 1) as i64, "error")?;
                            eprintln!(
                                "error: chunk {} not found on hub for {fp}",
                                chunk.hash.to_hex()
                            );
                            file_ok = false;
                            break;
                        }
                    }

                    catalog.update_job_progress(&job_id, (i + 1) as i64, "running")?;
                }

                if file_ok {
                    catalog.update_job_progress(&job_id, chunks.len() as i64, "completed")?;
                }
            }

            eprintln!(
                "done: {} fetched, {} skipped, {} bytes transferred ({} files)",
                total_fetched,
                total_skipped,
                total_bytes_transferred,
                file_paths.len()
            );
        }
    }

    Ok(())
}
