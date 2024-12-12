use anyhow::Result;
use async_compression::tokio::write::ZstdEncoder;
use chrono::{DateTime, Utc};
use log::{error, info};
use nostr_relay_builder::builder::{PolicyResult, QueryPolicy, RateLimit};
use nostr_relay_builder::prelude::{
    async_trait, Backend, Coordinate, DatabaseError, DatabaseEventStatus, Event, EventId, Events,
    JsonUtil, NostrDatabase, RejectedReason, Timestamp,
};
use nostr_relay_builder::prelude::{
    Filter as RelayFilter, NostrEventsDatabase, RelayUrl, SaveEventStatus,
};
use nostr_relay_builder::{LocalRelay, RelayBuilder};
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::fs::create_dir_all;
use std::io::{Error, ErrorKind};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use warp::reply::html;
use warp::Filter;

#[derive(Debug)]
struct NoQuery;

#[async_trait]
impl QueryPolicy for NoQuery {
    async fn admit_query(&self, _query: &[RelayFilter], _addr: &SocketAddr) -> PolicyResult {
        PolicyResult::Reject("queries not allowed".to_string())
    }
}

struct FlatFileWriter {
    pub dir: PathBuf,
    pub current_date: DateTime<Utc>,
    pub current_handle: Option<(PathBuf, File)>,
}

impl FlatFileWriter {
    /// Spawn a task to compress a file
    async fn compress_file(file: PathBuf) -> Result<()> {
        let out_path = file.with_extension("jsonl.zstd");
        let mut in_file = File::open(file.clone()).await?;
        let out_file = File::create(out_path.clone()).await?;
        let mut enc = ZstdEncoder::new(out_file);

        let mut buf: [u8; 1024] = [0; 1024];
        while let Ok(n) = in_file.read(&mut buf).await {
            if n == 0 {
                break;
            }
            enc.write_all(&buf[..n]).await?;
        }
        enc.flush().await?;
        drop(enc);

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
    async fn write_event(&mut self, ev: &Event) -> Result<()> {
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

        if let Some((_path, ref mut handle)) = self.current_handle.as_mut() {
            handle.write_all(ev.as_json().as_bytes()).await?;
            handle.write(b"\n").await?;
        }
        Ok(())
    }
}

struct FlatFileDatabase {
    database: sled::Db,
    dir: PathBuf,
    file: Arc<Mutex<FlatFileWriter>>,
}

impl Debug for FlatFileDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl FlatFileDatabase {
    pub fn new(dir: PathBuf) -> Result<Self> {
        create_dir_all(&dir)?;
        let db = sled::open(dir.join("index"))?;
        Ok(Self {
            dir: dir.clone(),
            database: db,
            file: Arc::new(Mutex::new(FlatFileWriter {
                dir,
                current_date: Utc::now(),
                current_handle: None,
            })),
        })
    }

    pub async fn write_event(&self, ev: &Event) -> Result<()> {
        self.file.lock().await.write_event(ev).await
    }
}

#[async_trait]
impl NostrEventsDatabase for FlatFileDatabase {
    async fn save_event(&self, event: &Event) -> Result<SaveEventStatus, DatabaseError> {
        match self.check_id(&event.id).await? {
            DatabaseEventStatus::NotExistent => {
                self.database
                    .insert(&event.id, &[])
                    .map_err(|e| DatabaseError::Backend(Box::new(e)))?;

                self.write_event(event).await.map_err(|e| {
                    DatabaseError::Backend(Box::new(Error::new(ErrorKind::Other, e)))
                })?;
                Ok(SaveEventStatus::Success)
            }
            _ => Ok(SaveEventStatus::Rejected(RejectedReason::Duplicate)),
        }
    }

    async fn check_id(&self, event_id: &EventId) -> Result<DatabaseEventStatus, DatabaseError> {
        if self
            .database
            .contains_key(event_id)
            .map_err(|e| DatabaseError::Backend(Box::new(e)))?
        {
            Ok(DatabaseEventStatus::Saved)
        } else {
            Ok(DatabaseEventStatus::NotExistent)
        }
    }

    async fn has_coordinate_been_deleted(
        &self,
        coordinate: &Coordinate,
        timestamp: &Timestamp,
    ) -> Result<bool, DatabaseError> {
        Ok(false)
    }

    async fn event_id_seen(
        &self,
        event_id: EventId,
        relay_url: RelayUrl,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    async fn event_seen_on_relays(
        &self,
        event_id: &EventId,
    ) -> Result<Option<HashSet<RelayUrl>>, DatabaseError> {
        Ok(None)
    }

    async fn event_by_id(&self, event_id: &EventId) -> Result<Option<Event>, DatabaseError> {
        Ok(None)
    }

    async fn count(&self, filters: Vec<RelayFilter>) -> Result<usize, DatabaseError> {
        Ok(0)
    }

    async fn query(&self, filters: Vec<RelayFilter>) -> Result<Events, DatabaseError> {
        Ok(Events::new(&[]))
    }

    async fn delete(&self, filter: RelayFilter) -> Result<(), DatabaseError> {
        Ok(())
    }
}

#[async_trait]
impl NostrDatabase for FlatFileDatabase {
    fn backend(&self) -> Backend {
        Backend::Custom("FlatFileDatabase".to_string())
    }

    async fn wipe(&self) -> Result<(), DatabaseError> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let out_dir = PathBuf::from("./data");
    let b = RelayBuilder::default()
        .database(FlatFileDatabase::new(out_dir.clone())?)
        .addr(IpAddr::from([0, 0, 0, 0]))
        .port(8001)
        .query_policy(NoQuery)
        .rate_limit(RateLimit {
            max_reqs: 20,
            notes_per_minute: 100_000,
        });

    let relay = LocalRelay::run(b).await?;

    info!("Relay started on: {}", relay.url());

    let template = include_str!("./index.html");
    let f = warp::get()
        .and(warp::path::end())
        .then(move || async move {
            let mut list = tokio::fs::read_dir("./data").await.unwrap();
            let mut files = Vec::new();
            while let Ok(Some(entry)) = list.next_entry().await {
                if entry.file_type().await.unwrap().is_dir() {
                    continue;
                }

                let ff = entry
                    .path()
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_string();
                let fs = entry.metadata().await.unwrap().len();
                files.push(format!("<a href=\"{ff}\">{ff} ({}M)</a>", fs / 1024 / 1024));
            }
            html(template.replace("%%_LINKS_%%", &files.join("\n")))
        })
        .or(warp::fs::dir(out_dir));

    let addr: SocketAddr = "0.0.0.0:8000".parse()?;
    let (addr, fut) = warp::serve(f).bind_with_graceful_shutdown(addr, async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen to shutdown signal");
    });
    info!("Listening on http://{}", addr);
    fut.await;
    relay.shutdown();

    Ok(())
}
