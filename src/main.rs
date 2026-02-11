use std::io;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

use rk_core::catalog::Catalog;
use rk_core::chunk_store::ChunkStore;
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
}

fn resolve_data_dir(raw: &str) -> PathBuf {
    if raw.starts_with("~/") {
        if let Some(home) = dirs_next::home_dir() {
            return home.join(&raw[2..]);
        }
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
            let (tape, prefix) = parse_tape_path(&path)?;
            let stdout = io::stdout();
            export::export_tar(stdout.lock(), &store, &catalog, "local", tape, prefix)
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
    }

    Ok(())
}
