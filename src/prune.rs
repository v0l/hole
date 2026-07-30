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
fn open_archive_reader(path: &Path) -> Result<Box<dyn BufRead>> {
    let input = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    if path.extension().and_then(|e| e.to_str()) == Some("zst") {
        let decoder = zstd::stream::Decoder::new(input)
            .with_context(|| format!("zstd decoder for {}", path.display()))?;
        Ok(Box::new(BufReader::new(decoder)))
    } else {
        Ok(Box::new(BufReader::new(input)))
    }
}

fn prune_file(path: &Path) -> Result<(u64, u64)> {
    let reader = open_archive_reader(path)?;

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

/// Count events per archive file, reporting kind mix and any read errors.
///
/// Read-only: opens nothing for writing. Use to find where events actually live
/// and which files are unreadable (e.g. truncated trailing zstd frame), since
/// those are silently skipped by the prune job.
pub async fn archive_stats(db: &DefaultJsonFilesDatabase) -> Result<()> {
    let mut files = db.list_files().await?;
    files.sort_by_key(|f| f.path.clone());

    let mut grand_total: u64 = 0;
    let mut grand_ephemeral: u64 = 0;
    let mut unreadable: Vec<(String, u64, String)> = Vec::new();

    for ArchiveFile { path, size, .. } in files {
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("").to_string();
        if !(name.ends_with(".jsonl.zst") || name.ends_with(".jsonl")) {
            continue;
        }

        let mut total = 0u64;
        let mut ephemeral = 0u64;
        let mut err: Option<String> = None;

        match open_archive_reader(&path) {
            Ok(reader) => {
                for line in reader.lines() {
                    match line {
                        Ok(l) => {
                            if l.trim().is_empty() {
                                continue;
                            }
                            total += 1;
                            if let Ok(ev) = serde_json::from_str::<KindOnly>(&l) {
                                if is_ephemeral_kind(ev.kind) {
                                    ephemeral += 1;
                                }
                            }
                        }
                        Err(e) => {
                            // Stops here: rest of the file is unreadable.
                            err = Some(e.to_string());
                            break;
                        }
                    }
                }
            }
            Err(e) => err = Some(format!("{e:#}")),
        }

        grand_total += total;
        grand_ephemeral += ephemeral;

        match &err {
            Some(e) => {
                warn!("{name}: {total} events read ({ephemeral} ephemeral), {size} bytes - READ ERROR: {e}");
                unreadable.push((name, total, e.clone()));
            }
            None => info!("{name}: {total} events ({ephemeral} ephemeral), {size} bytes"),
        }
    }

    info!("---");
    info!(
        "archive totals: {grand_total} events readable ({grand_ephemeral} ephemeral, {} non-ephemeral)",
        grand_total - grand_ephemeral
    );
    if !unreadable.is_empty() {
        warn!(
            "{} file(s) stop early on a read error - events after the error point are NOT counted above:",
            unreadable.len()
        );
        for (name, read, e) in &unreadable {
            warn!("  {name}: stopped after {read} events ({e})");
        }
    }

    Ok(())
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
    let mut failed: Vec<(String, String)> = Vec::new();
    let mut skipped_today = 0u64;

    for ArchiveFile { path, .. } in files {
        // Only handle .zst archive files, and skip today's (still being written).
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if !name.ends_with(".jsonl.zst") {
            continue;
        }
        if name.contains(&today) {
            info!("skipping today's active file: {name}");
            skipped_today += 1;
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
                failed.push((name.to_string(), format!("{e:#}")));
            }
        }
    }

    info!(
        "prune complete: dropped {total_dropped} ephemeral events across {files_touched} files ({total_kept} kept)"
    );
    info!(
        "files: {} rewritten, {} failed/skipped-on-error, {} skipped as today's active file",
        files_touched,
        failed.len(),
        skipped_today
    );

    // Failed files were left untouched (originals preserved) and are still
    // un-pruned, so their events are missing from the totals above. Surface them
    // explicitly - a truncated trailing zstd frame ("incomplete frame", from a
    // process killed mid-write) is the common cause.
    if !failed.is_empty() {
        warn!(
            "{} file(s) were NOT pruned and remain unchanged on disk:",
            failed.len()
        );
        for (name, err) in &failed {
            warn!("  {name}: {err}");
        }
    }

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
