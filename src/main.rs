use crate::http::HttpServer;
use crate::policy::{EphemeralPolicy, KindPolicy, NoQuery};
use anyhow::Result;
use clap::Parser;
use config::Config;
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use log::{error, info};
use nostr_archive_cursor::JsonFilesDatabase;
use nostr_relay_builder::builder::RateLimit;
use nostr_relay_builder::prelude::Kind;
use nostr_relay_builder::{LocalRelay, RelayBuilder};
use nostr_sdk::prelude::NostrDatabase;
use nostr_sdk::{Client, Filter, RelayPoolNotification};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

mod http;
mod policy;

#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Define path for config file
    pub config: Option<PathBuf>,
}

#[derive(Deserialize)]
struct Settings {
    /// Listen address for relay ip:port
    pub listen_relay: Option<String>,

    /// Nostr relays to ingest events from
    pub relays: Option<Vec<String>>,

    /// Nostr kinds to accept
    pub kinds: Option<Vec<u32>>,

    /// Path to save data
    pub out_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let config: Settings = Config::builder()
        .add_source(config::File::from(
            args.config.unwrap_or(PathBuf::from("config.yaml")),
        ))
        .build()?
        .try_deserialize()?;

    let out_dir = config.out_dir.unwrap_or(PathBuf::from("./data"));
    let addr: SocketAddr = config
        .listen_relay
        .map(|a| a.parse())
        .unwrap_or(Ok(SocketAddr::from(([0, 0, 0, 0], 8001))))?;

    let mut db = JsonFilesDatabase::new(out_dir.clone())?;

    // rebuild index if needed
    if db.is_index_empty() && !db.list_files().await?.is_empty() {
        info!("Index is empty, rebuilding....");
        db.rebuild_index()?;
    }

    let client = Client::builder().database(db.clone()).build();
    if let Some(r) = config.relays {
        for r in &r {
            client.add_relay(r).await?;
        }
        client.connect().await;

        let mut filter_base = Filter::default();
        if let Some(k) = &config.kinds {
            filter_base = filter_base.kinds(k.iter().map(|v| Kind::Custom(*v as u16)))
        }

        // spawn main ingester
        let client_sub = client.clone();
        let db_sub = db.clone();
        let filter_sub = filter_base.clone();
        let _: JoinHandle<Result<()>> = tokio::spawn(async move {
            let mut rx = client_sub.notifications();
            client_sub.subscribe(filter_sub.limit(100), None).await?;
            loop {
                match rx.recv().await {
                    Ok(e) => match e {
                        RelayPoolNotification::Event { event, .. } => {
                            if let Err(e) = db_sub.save_event(&event).await {
                                error!("Failed to save event: {}", e);
                            }
                        }
                        RelayPoolNotification::Message { .. } => {}
                        RelayPoolNotification::Shutdown => {}
                    },
                    Err(e) => {
                        error!("Client error notification: {}", e);
                        if matches!(e, RecvError::Closed) {
                            break;
                        }
                    }
                }
            }
            error!("Read loop exited!");
            Ok(())
        });
    }

    let mut builder = RelayBuilder::default()
        .database(db.clone())
        .query_policy(NoQuery)
        .write_policy(EphemeralPolicy)
        .rate_limit(RateLimit {
            max_reqs: 20,
            notes_per_minute: 100_000,
        });
    if let Some(k) = &config.kinds {
        builder = builder.write_policy(KindPolicy::new(
            k.iter().map(|k| Kind::Custom(*k as u16)).collect(),
        ));
    }
    let relay = LocalRelay::new(builder);

    let listener = TcpListener::bind(&addr).await?;
    info!("Listening on {}", &addr);
    loop {
        let (socket, addr) = listener.accept().await?;

        let io = TokioIo::new(socket);
        let server = HttpServer::new(relay.clone(), db.clone(), addr);
        tokio::spawn(async move {
            if let Err(e) = http1::Builder::new()
                .serve_connection(io, server)
                .with_upgrades()
                .await
            {
                error!("Failed to handle request: {}", e);
            }
        });
    }
}
