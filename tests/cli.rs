//! Behaviour of the real binary: environment handling and graceful termination.
#![allow(missing_docs)]

mod common;

use common::*;
use ferrum_db::resp::Frame;

#[tokio::test]
async fn an_empty_password_variable_means_no_password() {
    // docker-compose passes `FERRUM_REQUIREPASS=` when the variable is unset on the host.
    let dir = tempfile::tempdir().unwrap();
    let mut proc = Proc::spawn_env(dir.path(), &[], &[("FERRUM_REQUIREPASS", "")]);
    let mut c = proc.ready().await;
    assert_eq!(c.cmd(&["PING"]).await, Frame::simple("PONG"));
}

#[tokio::test]
async fn a_password_from_the_environment_is_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let mut proc = Proc::spawn_env(dir.path(), &[], &[("FERRUM_REQUIREPASS", "replace-me")]);
    let mut c = proc.ready().await;
    assert!(matches!(c.cmd(&["PING"]).await, Frame::Error(e) if e.starts_with("NOAUTH")));
    assert_eq!(c.cmd(&["AUTH", "replace-me"]).await, ok());
    assert_eq!(c.cmd(&["PING"]).await, Frame::simple("PONG"));
}

#[tokio::test]
async fn invalid_flags_are_rejected_before_binding() {
    let dir = tempfile::tempdir().unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_ferrum-db"))
        .args(["--aof-fsync", "sometimes", "--dir"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("expected always, everysec or no"));
}

#[tokio::test]
async fn sigterm_shuts_down_gracefully_and_flushes_the_aof() {
    let dir = tempfile::tempdir().unwrap();
    let mut proc = Proc::spawn(dir.path(), &["--aof", "--aof-fsync", "everysec"]);
    let mut c = proc.ready().await;
    for i in 0..50 {
        c.cmd(&["SET", &format!("k{i}"), "v"]).await;
    }
    // everysec may still hold the last writes in memory; a graceful stop must flush them.
    proc.terminate();
    let status = proc.wait_exit().await;
    assert!(
        status.success(),
        "graceful shutdown must exit 0, got {status}"
    );

    let mut proc = Proc::spawn(dir.path(), &["--aof"]);
    let mut c = proc.ready().await;
    assert_eq!(c.cmd(&["DBSIZE"]).await, int(50));
}
