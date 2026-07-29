//! One-shot job to prune ephemeral events (kind 20000-29999) from completed
//! archive files by rewriting them without those events.
//!
//! Only files for days *before* today are rewritten - today's file is still being
//! appended by the live writer, so we leave it alone (and it's out of scope for a
//! startup job). After rewriting, the index count is repaired to match.

use anyhow::{Context, Result, anyhow};
use log::{info, warn};
use nostr_archive_cursor::{ArchiveFile, DefaultJsonFilesDatabase};
use serde::Deserialize;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

/// Minimal view of an event line - we only need `kind` to decide keep/drop.
/// serde skips all other fields without allocating for them.
#[derive(Deserialize)]
struct KindOnly {
    kind: u32,
}

#[inline]
fn is_ephemeral_kind(kind: u32) -> bool {
    (20_000..30_000).contains(&kind)
}

/// Rewrite a single `.jsonl.zst` archive file, dropping ephemeral events.
///
/// Streams decompressed input line-by-line, writes kept lines to a temp file,
/// then atomically renames it over the original. Returns (kept, dropped).
fn prune_file(path: &Path) -> Result<(u64, u64)> {
    let input = std::fs::File::open(path)
        .with_context(|| format!("open {}", path.display()))?;
    let decoder = zstd::stream::Decoder::new(input)
        .with_context(|| format!("zstd decoder for {}", path.display()))?;
    let reader = BufReader::new(decoder);

    // Temp file alongside the original (same filesystem -> atomic rename).
    let tmp_path = path.with_extension("jsonl.zst.prune-tmp");
    let tmp_file = std::fs::File::create(&tmp_path)
        .with_context(|| format!("create {}", tmp_path.display()))?;
    let buf_writer = BufWriter::new(tmp_file);
    // Match the writer's compression level (3) used by the archive writer.
    let mut encoder = zstd::stream::Encoder::new(buf_writer, 3)
        .with_context(|| "zstd encoder")?;

    let mut kept: u64 = 0;
    let mut dropped: u64 = 0;
    let mut bad_lines: u64 = 0;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<KindOnly>(&line) {
            Ok(ev) if is_ephemeral_kind(ev.kind) => {
                dropped += 1;
            }
            Ok(_) => {
                encoder.write_all(line.as_bytes())?;
                encoder.write_all(b"\n")?;
                kept += 1;
            }
            Err(e) => {
                // Unparseable line: keep it (don't lose data) but count it.
                bad_lines += 1;
                encoder.write_all(line.as_bytes())?;
                encoder.write_all(b"\n")?;
                warn!("unparseable line kept in {}: {}", path.display(), e);
            }
        }
    }

    // Finalize the zstd frame and flush the underlying BufWriter.
    let buf_writer = encoder.finish().context("finish zstd frame")?;
    let mut tmp_file = buf_writer.into_inner().map_err(|e| anyhow!(e))?;
    tmp_file.flush()?;
    drop(tmp_file);

    if dropped == 0 {
        // Nothing to prune; keep the original untouched.
        let _ = std::fs::remove_file(&tmp_path);
        return Ok((kept, 0));
    }

    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("rename {} -> {}", tmp_path.display(), path.display()))?;

    if bad_lines > 0 {
        warn!("{}: {} unparseable lines kept", path.display(), bad_lines);
    }

    Ok((kept, dropped))
}

/// Prune ephemeral events from all completed (non-today) archive files, then
/// repair the index count. Returns total events dropped.
pub async fn prune_ephemeral(db: &DefaultJsonFilesDatabase) -> Result<u64> {
    let files = db.list_files().await?;

    // Determine "today" using the same naming the writer uses, so we skip the
    // file currently being appended.
    let today = chrono::Utc::now().format(DefaultJsonFilesDatabase::EVENT_FORMAT).to_string();

    let mut total_dropped: u64 = 0;
    let mut total_kept: u64 = 0;
    let mut files_touched: u64 = 0;

    for ArchiveFile { path, .. } in files {
        // Only handle .zst archive files, and skip today's (still being written).
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if !name.ends_with(".jsonl.zst") {
            continue;
        }
        if name.contains(&today) {
            info!("skipping today's active file: {name}");
            continue;
        }

        info!("pruning {name}...");
        match prune_file(&path) {
            Ok((kept, dropped)) => {
                total_kept += kept;
                total_dropped += dropped;
                if dropped > 0 {
                    files_touched += 1;
                    info!("{name}: kept {kept}, dropped {dropped} ephemeral");
                } else {
                    info!("{name}: no ephemeral events");
                }
            }
            Err(e) => {
                warn!("failed to prune {name}: {e:#}");
            }
        }
    }

    info!(
        "prune complete: dropped {total_dropped} ephemeral events across {files_touched} files ({total_kept} kept)"
    );

    // The index still holds the pruned (ephemeral) ids. Rebuild it from the now-clean
    // archive files so both the ids and the cached count reflect reality. The rebuild
    // reads every archive file; today's file may end in an unfinished zstd frame (it is
    // not being written while this job runs), in which case its un-flushed tail is
    // skipped - those events are re-ingested live on the next run.
    if total_dropped > 0 {
        info!("rebuilding index from pruned archives...");
        let mut db_mut = db.clone();
        db_mut.rebuild_index()?;
        info!("index rebuilt: {} events", db.count_keys());
    }

    Ok(total_dropped)
}
