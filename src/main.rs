// SPDX-License-Identifier: AGPL-3.0-only

use std::io;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

use rk_core::catalog::Catalog;
use rk_core::chunk_store::ChunkStore;
use rk_core::indexer::IndexConfig;
use rk_core::manifest::Manifest;
use rk_core::resolver::ChunkResolver;
use rk_core::verifier;
use rk_tar::ingest::{self, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE};
use rk_tar::export;

#[derive(Parser)]
#[command(name = "rk", about = "Content-addressed file delivery over hostile networks")]
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

fn main() -> Result<()> {
    let cli = Cli::parse();
    let data_dir = resolve_data_dir(&cli.data_dir);
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("creating data directory: {}", data_dir.display()))?;

    let store = ChunkStore::new(data_dir.clone());
    let catalog = Catalog::open(data_dir.join("catalog.db").as_path())
        .context("opening catalog")?;

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
            let (tape, prefix) = parse_tape_path(&path)?;
            let files = catalog
                .list_files("local", tape, prefix)
                .context("listing files")?;
            for f in &files {
                let kind = if f.entry_type == 2 { "d" } else { "-" };
                let mode = f.mode.unwrap_or(0);
                println!("{}{:03o}  {:>10}  {}", kind, mode, f.size, f.path);
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
                        j.job_id, j.status, j.job_type,
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
            let stats = rk_core::indexer::index_directory(
                &dir, &mut m, &catalog, "local", &tape, &config,
            )
            .context("indexing directory")?;
            m.write_to_file(&manifest_path).context("writing manifest")?;
            eprintln!(
                "indexed {} files, {} dirs, {} chunks ({} new, {} dedup), {} bytes",
                stats.files_indexed, stats.dirs_found,
                stats.chunks_total, stats.chunks_new, stats.chunks_dedup,
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
                    result.chunks_stale, result.chunks_missing, result.chunks_corrupted
                );
            }
        }
    }

    Ok(())
}
