use crate::db::FlatFileDatabase;
use crate::http::HttpServer;
use crate::policy::{KindPolicy, NoQuery};
use anyhow::Result;
use clap::Parser;
use config::Config;
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use log::{error, info};
use nostr_relay_builder::builder::RateLimit;
use nostr_relay_builder::prelude::Kind;
use nostr_relay_builder::prelude::NostrEventsDatabase;
use nostr_relay_builder::{LocalRelay, RelayBuilder};
use nostr_sdk::prelude::{ReqExitPolicy, StreamExt};
use nostr_sdk::Client;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::net::TcpListener;

mod db;
mod http;
mod policy;
mod writer;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let args = Args::parse();

    let config: Settings = Config::builder()
        .add_source(config::File::from(
            args.config.unwrap_or(PathBuf::from("config.yaml")),
        ))
        .build()?
        .try_deserialize()?;

    let out_dir = PathBuf::from("./data");
    let addr: SocketAddr = config
        .listen_relay
        .map(|a| a.parse())
        .unwrap_or(Ok(SocketAddr::from(([0, 0, 0, 0], 8001))))?;

    let db = FlatFileDatabase::new(out_dir.clone())?;
    let client = Client::builder().database(db.clone()).build();
    if let Some(r) = config.relays {
        for r in r {
            client.add_relay(r).await?;
        }
        client.connect().await;
        let never = Duration::from_secs(u64::MAX);
        let mut stream = client
            .pool()
            .stream_events(
                nostr_sdk::Filter::new().limit(100),
                never,
                ReqExitPolicy::WaitDurationAfterEOSE(never),
            )
            .await?;
        let db = db.clone();
        tokio::spawn(async move {
            while let Some(event) = stream.next().await {
                if let Err(e) = db.save_event(&event).await {
                    error!("Failed to save event: {}", e);
                }
            }
        });
    }

    let mut builder = RelayBuilder::default()
        .database(db.clone())
        .query_policy(NoQuery)
        .rate_limit(RateLimit {
            max_reqs: 20,
            notes_per_minute: 100_000,
        });
    if let Some(k) = &config.kinds {
        builder = builder.write_policy(KindPolicy::new(
            k.iter().map(|k| Kind::Custom(*k as u16)).collect(),
        ));
    }
    let relay = LocalRelay::new(builder).await?;

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
