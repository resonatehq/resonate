//! Against a real `msb`. Skipped unless `MSB` names the binary:
//!
//! ```sh
//! MSB=$(which msb) cargo test -p resonate-sandbox-microsandbox --test live
//! ```
//!
//! Booting needs KVM (or macOS on Apple Silicon); where there is none,
//! `boots_runs_and_destroys` says so and skips, and the rest still run.

use std::process::Stdio;

use resonate_sandbox::{Backend, Command, Egress, Limits, Process};
use resonate_sandbox_microsandbox::Microsandbox;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const IMAGE: &str = "alpine:3.20";

fn msb() -> Option<String> {
    std::env::var("MSB").ok().filter(|s| !s.is_empty())
}

fn can_boot() -> bool {
    cfg!(target_os = "macos") || std::path::Path::new("/dev/kvm").exists()
}

/// Sandboxes this backend made that still exist.
async fn leftovers(msb: &str) -> usize {
    let out = tokio::process::Command::new(msb)
        .args(["ls", "--label", "resonate=sandbox", "--format", "json"])
        .stdin(Stdio::null())
        .output()
        .await
        .expect("msb ls");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice::<Vec<serde_json::Value>>(&out.stdout)
        .expect("msb ls --format json is a list")
        .len()
}

#[tokio::test]
async fn every_egress_is_accepted_by_msb_and_a_failed_create_leaves_nothing() {
    let Some(msb) = msb() else { return };
    for egress in [
        Egress::None,
        Egress::All,
        Egress::Allow(vec!["books.toscrape.com".into(), "10.0.0.0/8".into()]),
    ] {
        let b = Microsandbox::new(
            &msb,
            Limits {
                cpus: Some(1),
                memory_mib: Some(256),
                egress: egress.clone(),
            },
        );
        match b.create(IMAGE).await {
            Ok(h) => b.destroy(h).await.expect("destroy"),
            // msb checks its flags before it boots: a usage error names the
            // flag; anything later is the machine.
            Err(e) => {
                let e = e.to_string();
                assert!(!can_boot(), "{egress:?}: {e}");
                assert!(
                    e.contains("failed to start"),
                    "{egress:?} was refused before boot: {e}"
                );
            }
        }
    }
    assert_eq!(leftovers(&msb).await, 0);
}

#[tokio::test]
async fn boots_runs_and_destroys() {
    let Some(msb) = msb() else { return };
    if !can_boot() {
        eprintln!("no /dev/kvm: not booting");
        return;
    }
    let b = Microsandbox::new(
        &msb,
        Limits {
            cpus: Some(1),
            memory_mib: Some(256),
            egress: Egress::None,
        },
    );
    let h = b.create(IMAGE).await.expect("create");

    // Live stdio, line by line, both ways — what the frames ride on.
    let mut p = b
        .exec(
            &h,
            Command {
                argv: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "while read l; do echo \"$K:$l\"; done; echo bye >&2; exit 3".into(),
                ],
                env: vec![("K".into(), "v".into())],
            },
        )
        .await
        .expect("exec");
    let mut stdin = p.stdin().expect("stdin");
    let mut out = BufReader::new(p.stdout().expect("stdout")).lines();
    let mut err = BufReader::new(p.stderr().expect("stderr")).lines();
    for i in 0..3 {
        stdin
            .write_all(format!("{{\"n\":{i}}}\n").as_bytes())
            .await
            .unwrap();
        stdin.flush().await.unwrap();
        assert_eq!(
            out.next_line().await.unwrap().unwrap(),
            format!("v:{{\"n\":{i}}}")
        );
    }
    drop(stdin);
    assert_eq!(out.next_line().await.unwrap(), None);
    assert_eq!(err.next_line().await.unwrap().as_deref(), Some("bye"));
    assert_eq!(p.wait().await.unwrap(), 3);

    // No network: the guest cannot reach anything.
    let mut p = b
        .exec(
            &h,
            Command {
                argv: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "wget -q -T 5 -O- http://1.1.1.1".into(),
                ],
                env: vec![],
            },
        )
        .await
        .expect("exec");
    assert_ne!(p.wait().await.unwrap(), 0);

    b.destroy(h).await.expect("destroy");
    assert_eq!(leftovers(&msb).await, 0);
}
