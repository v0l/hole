use crate::writer::FlatFileWriter;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use log::debug;
use nostr_relay_builder::prelude::BoxedFuture;
use nostr_sdk::prelude::{
    Backend, DatabaseError, DatabaseEventStatus, Events, NostrDatabase, RejectedReason,
    SaveEventStatus,
};
use nostr_sdk::{Event, EventId, Filter};
use std::fmt::{Debug, Formatter};
use std::fs::create_dir_all;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct FlatFileDatabase {
    out_dir: PathBuf,
    database: sled::Db,
    file: Arc<Mutex<FlatFileWriter>>,
}

impl Debug for FlatFileDatabase {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ArchiveFile {
    pub path: PathBuf,
    pub size: u64,
    pub created: DateTime<Utc>,
}

impl FlatFileDatabase {
    pub fn new(dir: PathBuf) -> Result<Self> {
        create_dir_all(&dir)?;
        let db = sled::open(dir.join("index"))?;
        Ok(Self {
            out_dir: dir.clone(),
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

    pub async fn list_files(&self) -> Result<Vec<ArchiveFile>> {
        let mut list = tokio::fs::read_dir(&self.out_dir).await?;
        let mut files = Vec::new();
        while let Ok(Some(entry)) = list.next_entry().await {
            if entry.file_type().await?.is_dir() {
                continue;
            }

            let meta = entry.metadata().await?;
            files.push(ArchiveFile {
                path: entry.path(),
                size: meta.len(),
                created: meta.created()?.into(),
            });
        }
        Ok(files)
    }

    /// Return archive file if it exists
    pub fn get_file(&self, path: &str) -> Result<ArchiveFile> {
        let p = self.out_dir.join(&path[1..]);
        if p.exists() && p.is_file() {
            let meta = p.metadata()?;
            Ok(ArchiveFile {
                path: p,
                size: meta.len(),
                created: meta.created()?.into(),
            })
        } else {
            Err(anyhow!("No such file or directory"))
        }
    }
}

impl NostrDatabase for FlatFileDatabase {
    fn backend(&self) -> Backend {
        Backend::Custom("FlatFileDatabase".to_owned())
    }

    fn save_event<'a>(
        &'a self,
        event: &'a Event,
    ) -> BoxedFuture<'a, Result<SaveEventStatus, DatabaseError>> {
        Box::pin(async move {
            match self.check_id(&event.id).await? {
                DatabaseEventStatus::NotExistent => {
                    self.database
                        .insert(event.id, &[])
                        .map_err(|e| DatabaseError::Backend(Box::new(e)))?;

                    self.write_event(event).await.map_err(|e| {
                        DatabaseError::Backend(Box::new(Error::new(ErrorKind::Other, e)))
                    })?;
                    debug!("Saved event: {}", event.id);
                    Ok(SaveEventStatus::Success)
                }
                _ => Ok(SaveEventStatus::Rejected(RejectedReason::Duplicate)),
            }
        })
    }

    fn check_id<'a>(
        &'a self,
        event_id: &'a EventId,
    ) -> BoxedFuture<'a, Result<DatabaseEventStatus, DatabaseError>> {
        Box::pin(async move {
            if self
                .database
                .contains_key(event_id)
                .map_err(|e| DatabaseError::Backend(Box::new(e)))?
            {
                Ok(DatabaseEventStatus::Saved)
            } else {
                Ok(DatabaseEventStatus::NotExistent)
            }
        })
    }

    fn event_by_id(
        &self,
        _event_id: &EventId,
    ) -> BoxedFuture<'_, Result<Option<Event>, DatabaseError>> {
        Box::pin(async move { Ok(None) })
    }

    fn count(&self, _filters: Filter) -> BoxedFuture<'_, Result<usize, DatabaseError>> {
        Box::pin(async move { Ok(0) })
    }

    fn query(&self, filter: Filter) -> BoxedFuture<'_, Result<Events, DatabaseError>> {
        Box::pin(async move { Ok(Events::new(&filter)) })
    }

    fn delete(&self, _filter: Filter) -> BoxedFuture<'_, Result<(), DatabaseError>> {
        Box::pin(async move { Ok(()) })
    }

    fn wipe(&self) -> BoxedFuture<'_, nostr_relay_builder::prelude::Result<(), DatabaseError>> {
        Box::pin(async move { Ok(()) })
    }
}
