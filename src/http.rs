use crate::db::FlatFileDatabase;
use base64::prelude::*;
use http_body_util::Either;
use hyper::body::{Body, Bytes, Frame, Incoming};
use hyper::header::{CONNECTION, SEC_WEBSOCKET_ACCEPT, UPGRADE};
use hyper::service::Service;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use itertools::Itertools;
use log::error;
use nostr_relay_builder::LocalRelay;
use nostr_sdk::prelude::StreamExt;
use sha1::Digest;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use thousands::Separable;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

pub(crate) struct HttpServer {
    relay: LocalRelay,
    db: FlatFileDatabase,
    remote: SocketAddr,
}

/// Copied from https://github.com/snapview/tungstenite-rs/blob/c16778797b2eeb118aa064aa5b483f90c3989627/src/handshake/mod.rs#L112C1-L125C1
/// Derive the `Sec-WebSocket-Accept` response header from a `Sec-WebSocket-Key` request header.
///
/// This function can be used to perform a handshake before passing a raw TCP stream to
/// [`WebSocket::from_raw_socket`][crate::protocol::WebSocket::from_raw_socket].
pub fn derive_accept_key(request_key: &[u8]) -> String {
    // ... field is constructed by concatenating /key/ ...
    // ... with the string "258EAFA5-E914-47DA-95CA-C5AB0DC85B11" (RFC 6455)
    const WS_GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    let mut content = Vec::new();
    content.extend_from_slice(request_key);
    content.extend_from_slice(WS_GUID);
    BASE64_STANDARD.encode(sha1::Sha1::digest(&content))
}

impl HttpServer {
    pub fn new(relay: LocalRelay, db: FlatFileDatabase, remote: SocketAddr) -> Self {
        HttpServer { relay, db, remote }
    }
}

impl Service<Request<Incoming>> for HttpServer {
    type Response = Response<Either<String, ArchiveFileReader>>;
    type Error = String;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let base = Response::builder()
            .header("server", "nostr-relay-builder")
            .status(404);

        // check is upgrade
        if let (Some(c), Some(w)) = (
            req.headers().get("connection"),
            req.headers().get("upgrade"),
        ) {
            if c.to_str()
                .map(|s| s.to_lowercase() == "upgrade")
                .unwrap_or(false)
                && w.to_str()
                    .map(|s| s.to_lowercase() == "websocket")
                    .unwrap_or(false)
            {
                let key = req.headers().get("sec-websocket-key");
                let derived = key.map(|k| derive_accept_key(k.as_bytes()));

                let addr = self.remote;
                let relay = self.relay.clone();
                tokio::spawn(async move {
                    match hyper::upgrade::on(req).await {
                        Ok(upgraded) => {
                            if let Err(e) =
                                relay.take_connection(TokioIo::new(upgraded), addr).await
                            {
                                error!("{}", e);
                            }
                        }
                        Err(e) => error!("{}", e),
                    }
                });
                return Box::pin(async move {
                    Ok(base
                        .status(101)
                        .header(CONNECTION, "upgrade")
                        .header(UPGRADE, "websocket")
                        .header(SEC_WEBSOCKET_ACCEPT, derived.unwrap())
                        .body(Either::Left(String::new()))
                        .unwrap())
                });
            }
        }

        // Check path is file path to serve file
        let path = req.uri().path();
        if path != "/" && path != "/index.html" {
            if let Ok(f) = self.db.get_file(path) {
                Box::pin(async move {
                    File::open(f.path)
                        .await
                        .map(|h| {
                            base.status(200)
                                .header("content-type", "application/octet-stream")
                                .header("content-length", f.size.to_string())
                                .body(Either::Right(ArchiveFileReader {
                                    handle: ReaderStream::new(h),
                                }))
                                .unwrap()
                        })
                        .map_err(|_| "Failed to open file".to_owned())
                })
            } else {
                Box::pin(async move { Ok(base.body(Either::Left(String::new())).unwrap()) })
            }
        } else {
            // serve landing page otherwise
            let template = include_str!("./index.html");
            let db = self.db.clone();
            Box::pin(async move {
                let files: Vec<(u64, String)> = db
                    .list_files()
                    .await
                    .unwrap()
                    .iter()
                    .sorted_by(|a, b| b.created.cmp(&a.created))
                    .map(|f| {
                        let name = f.path.file_name().unwrap().to_str().unwrap();
                        (f.size, String::from(name))
                    })
                    .collect();

                Ok(base
                    .status(200)
                    .header("content-type", "text/html")
                    .body(Either::Left(
                        template
                            .replace(
                                "%%_LINKS_%%",
                                &files
                                    .iter()
                                    .map(|f| {
                                        format!(
                                            "<a href=\"{}\">{} ({:.2} MiB)</a>",
                                            f.1,
                                            f.1,
                                            f.0 as f64 / 1024. / 1024.
                                        )
                                    })
                                    .collect::<Vec<_>>()
                                    .join("\n"),
                            )
                            .replace(
                                "%%_TOTAL_EVENTS_%%",
                                db.count_keys().separate_with_commas().as_str(),
                            )
                            .replace(
                                "%%_TOTAL_SIZE_%%",
                                &format!(
                                    "{:.3} GiB",
                                    files.iter().fold(0u64, |acc, v| acc + v.0) as f64
                                        / 1024.0
                                        / 1024.0
                                        / 1024.0
                                ),
                            ),
                    ))
                    .unwrap())
            })
        }
    }
}

pub struct ArchiveFileReader {
    pub handle: ReaderStream<File>,
}

impl Body for ArchiveFileReader {
    type Data = Bytes;
    type Error = String;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.handle.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(data))) => Poll::Ready(Some(Ok(Frame::data(data)))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.to_string()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
