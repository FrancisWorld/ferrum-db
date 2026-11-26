//! Malformed-input resilience: no command may panic on missing arguments, and a
//! misbehaving client must never leak a connection slot.
#![allow(missing_docs)]

mod common;

use std::time::Duration;

use common::*;
use ferrum_db::resp::Frame;

/// Sends `parts` on a fresh connection; the server must answer (any frame) rather than
/// drop the connection because a handler panicked.
async fn probe(srv: &TestServer, parts: &[&str]) -> Frame {
    let mut c = srv.client().await;
    c.send(parts).await;
    c.try_read(Duration::from_secs(5))
        .await
        .unwrap_or_else(|| panic!("no reply (handler panic?) for {parts:?}"))
}

#[tokio::test]
async fn every_command_survives_missing_and_minimal_arguments() {
    let srv = TestServer::start().await;
    let mut names: Vec<String> = ferrum_db::cmd::command_names()
        .into_iter()
        .map(String::from)
        .collect();
    names.extend(
        ferrum_db::session::SESSION_COMMANDS
            .iter()
            .map(|s| (*s).to_string()),
    );
    assert!(names.len() > 100);
    for name in &names {
        let min_args = ferrum_db::cmd::lookup(name.as_bytes())
            .map_or(2, |c| c.arity.unsigned_abs() as usize - 1);
        for argc in [0, 1, 2, min_args, min_args + 1] {
            let mut parts: Vec<&str> = vec![name.as_str()];
            parts.extend(std::iter::repeat_n("x", argc));
            probe(&srv, &parts).await;
        }
        // Mixed case reaches the same handlers.
        probe(&srv, &[&name.to_lowercase()]).await;
    }
    // The server is still healthy and no slot leaked.
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["PING"]).await, Frame::simple("PONG"));
    eventually(
        Duration::from_secs(5),
        "client slots to be released",
        || {
            srv.engine
                .stats()
                .clients
                .load(std::sync::atomic::Ordering::Relaxed)
                == 1
        },
    )
    .await;
}

#[tokio::test]
async fn bare_client_is_an_arity_error_and_does_not_exhaust_max_clients() {
    let srv = TestServer::start_with(|c| c.max_clients = 3).await;
    for _ in 0..30 {
        let mut c = srv.client().await;
        assert!(
            matches!(c.cmd(&["CLIENT"]).await, Frame::Error(e) if e.contains("wrong number of arguments for 'client'"))
        );
    }
    let mut c = srv.client().await;
    assert_eq!(c.cmd(&["PING"]).await, Frame::simple("PONG"));
}
