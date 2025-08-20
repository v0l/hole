use nostr_relay_builder::prelude::{PolicyResult, QueryPolicy, WritePolicy};
use nostr_sdk::prelude::BoxedFuture;
use nostr_sdk::{Event, Filter, Kind};
use std::collections::HashSet;
use std::net::SocketAddr;

#[derive(Debug)]
pub struct NoQuery;

impl QueryPolicy for NoQuery {
    fn admit_query(&self, _query: &Filter, _addr: &SocketAddr) -> BoxedFuture<'_, PolicyResult> {
        Box::pin(async move { PolicyResult::Reject("queries not allowed".to_string()) })
    }
}

#[derive(Debug)]
pub struct KindPolicy(HashSet<Kind>);

impl KindPolicy {
    pub fn new(kinds: HashSet<Kind>) -> Self {
        Self(kinds)
    }
}

impl WritePolicy for KindPolicy {
    fn admit_event<'a>(
        &'a self,
        event: &'a Event,
        _addr: &SocketAddr,
    ) -> BoxedFuture<'a, PolicyResult> {
        Box::pin(async move {
            if self.0.contains(&event.kind) {
                PolicyResult::Accept
            } else {
                PolicyResult::Reject("Kind not accepted".to_string())
            }
        })
    }
}
