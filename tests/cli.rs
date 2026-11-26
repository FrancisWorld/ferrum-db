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

#[test]
fn refuses_to_listen_on_the_network_without_a_password() {
    let dir = tempfile::tempdir().unwrap();
    let run = |args: &[&str], envs: &[(&str, &str)]| {
        let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_ferrum-db"))
            .args(["--port", "0", "--bind", "0.0.0.0", "--dir"])
            .arg(dir.path())
            .args(args)
            .envs(envs.iter().copied())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(700));
        let exited = child.try_wait().unwrap();
        if exited.is_none() {
            child.kill().unwrap();
        }
        let status = child.wait().unwrap();
        let mut stderr = String::new();
        std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut stderr).unwrap();
        (exited.is_some(), status, stderr)
    };
    let (exited, status, stderr) = run(&[], &[]);
    assert!(exited && !status.success(), "must refuse to start");
    assert!(
        stderr.contains("refusing to listen on 0.0.0.0 without a password"),
        "{stderr}"
    );
    // An empty password (what an unset compose variable becomes) is still no password.
    assert!(run(&[], &[("FERRUM_REQUIREPASS", "")]).0);
    // With a password, or an explicit and loudly logged opt-out, it starts.
    assert!(!run(&["--requirepass", "replace-me"], &[]).0);
    let (exited, _, stderr) = run(&[], &[("FERRUM_ALLOW_NO_AUTH", "1")]);
    assert!(!exited, "opt-out must start");
    assert!(stderr.contains("RUNNING WITHOUT A PASSWORD"), "{stderr}");
}

#[tokio::test]
async fn a_stalled_giant_bulk_declaration_neither_allocates_nor_buffers_unboundedly() {
    let dir = tempfile::tempdir().unwrap();
    let mut proc = Proc::spawn(
        dir.path(),
        &["--max-bulk-len", "512mb", "--max-query-buffer", "1mb"],
    );
    let mut probe = proc.ready().await;
    let rss_kb = |pid: u32| -> u64 {
        let out = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().parse().unwrap()
    };
    let before = rss_kb(proc.pid());
    // Sixteen connections each declare a 512 MiB value and then stall.
    let mut stalled = Vec::new();
    for _ in 0..16 {
        let mut c = proc.client().await;
        c.send_raw(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$536870912\r\nabc")
            .await;
        stalled.push(c);
    }
    assert_eq!(probe.cmd(&["PING"]).await, Frame::simple("PONG"));
    let grown_mib = (rss_kb(proc.pid()).saturating_sub(before)) / 1024;
    assert!(
        grown_mib < 64,
        "RSS grew by {grown_mib} MiB for stalled declarations"
    );

    // A peer that actually streams data hits the per-connection buffer cap.
    let mut flood = proc.client().await;
    flood
        .send_raw(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$536870912\r\n")
        .await;
    let chunk = vec![b'x'; 256 * 1024];
    for _ in 0..16 {
        if !flood.try_send_raw(&chunk).await {
            break; // the server already hung up
        }
    }
    // The error may be lost if the server resets the connection first; either way it ends.
    if let Some(Frame::Error(e)) = flood.try_read(std::time::Duration::from_secs(5)).await {
        assert!(e.contains("query buffer limit exceeded"), "{e}");
    }
    assert!(flood.is_closed().await);
    assert_eq!(probe.cmd(&["PING"]).await, Frame::simple("PONG"));
}
