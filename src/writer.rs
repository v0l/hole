use anyhow::Result;
use async_compression::tokio::write::ZstdEncoder;
use chrono::{DateTime, Utc};
use log::{error, info};
use nostr_sdk::{Event, JsonUtil};
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct FlatFileWriter {
    pub dir: PathBuf,
    pub current_date: DateTime<Utc>,
    pub current_handle: Option<(PathBuf, File)>,
}

impl FlatFileWriter {
    /// Spawn a task to compress a file
    async fn compress_file(file: PathBuf) -> Result<()> {
        let out_path = file.with_extension("jsonl.zstd");
        let mut in_file = File::open(file.clone()).await?;
        {
            let out_file = File::create(out_path.clone()).await?;
            let mut enc = ZstdEncoder::new(out_file);
            let mut buf: [u8; 1024] = [0; 1024];
            while let Ok(n) = in_file.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                enc.write_all(&buf[..n]).await?;
            }
            enc.shutdown().await?;
        }

        let in_size = in_file.metadata().await?.len();
        let out_size = File::open(out_path).await?.metadata().await?.len();
        drop(in_file);
        tokio::fs::remove_file(file).await?;
        info!(
            "Compressed file ratio={:.2}x, size={}M",
            in_size as f32 / out_size as f32,
            out_size as f32 / 1024.0 / 1024.0
        );

        Ok(())
    }

    /// Write event to the current file handle, or move to the next file handle
    pub(crate) async fn write_event(&mut self, ev: &Event) -> Result<()> {
        const EVENT_FORMAT: &str = "%Y%m%d";
        let now = Utc::now();
        if self.current_date.format(EVENT_FORMAT).to_string()
            != now.format(EVENT_FORMAT).to_string()
        {
            if let Some((path, ref mut handle)) = self.current_handle.take() {
                handle.flush().await?;
                info!("Closing file {:?}", &path);
                tokio::spawn(async move {
                    if let Err(e) = Self::compress_file(path).await {
                        error!("Failed to compress file: {}", e);
                    }
                });
            }

            // open new file
            self.current_date = now;
        }

        if self.current_handle.is_none() {
            let path = self.dir.join(format!(
                "events_{}.jsonl",
                self.current_date.format(EVENT_FORMAT)
            ));
            info!("Creating file {:?}", &path);
            self.current_handle = Some((
                path.clone(),
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(path)
                    .await?,
            ));
        }

        if let Some((_path, handle)) = self.current_handle.as_mut() {
            handle.write_all(ev.as_json().as_bytes()).await?;
            handle.write(b"\n").await?;
        }
        Ok(())
    }
}
