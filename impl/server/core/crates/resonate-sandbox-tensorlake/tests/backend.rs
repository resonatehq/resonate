//! The backend against a fake Tensorlake: create, a process with live stdio,
//! its exit status, destroy.

use std::time::Duration;

use resonate_sandbox::{Backend, Command, Egress, Limits, Process};
use resonate_sandbox_tensorlake::testing::{FakeTensorlake, KEY};
use resonate_sandbox_tensorlake::{Options, Tensorlake};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn backend(fake: &FakeTensorlake) -> Tensorlake {
    Tensorlake::new(
        Options {
            api_url: fake.url.clone(),
            api_key: Some(KEY.into()),
            poll_interval: Duration::from_millis(10),
            ..Options::default()
        },
        Limits {
            cpus: Some(1),
            memory_mib: Some(256),
            egress: Egress::None,
        },
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_process_streams_both_ways_and_reports_its_exit() {
    let fake = FakeTensorlake::start().await;
    let tl = backend(&fake);

    let handle = tl.create("ghcr.io/a/b@sha256:00").await.expect("created");
    let created = fake.seen(|s| s.creates[0].clone());
    assert_eq!(created["image"], "ghcr.io/a/b@sha256:00");
    assert_eq!(created["network"]["allow_internet_access"], false);
    assert_eq!(created["resources"]["memory_mb"], 256);

    // Echo stdin to stdout until EOF, say something on stderr, exit 3.
    let mut p = tl
        .exec(
            &handle,
            Command {
                argv: vec![
                    "sh".into(),
                    "-c".into(),
                    "while read l; do echo \"got $l\"; done; echo \"$GREETING\" >&2; exit 3".into(),
                ],
                env: vec![("GREETING".into(), "bye".into())],
            },
        )
        .await
        .expect("started");
    let mut stdin = p.stdin().unwrap();
    let mut stdout = BufReader::new(p.stdout().unwrap()).lines();
    let mut stderr = BufReader::new(p.stderr().unwrap()).lines();

    // Live, not batched: each answer arrives before the next line is sent.
    for i in 0..3 {
        stdin
            .write_all(format!("{{\"n\":{i}}}\n").as_bytes())
            .await
            .unwrap();
        stdin.flush().await.unwrap();
        let line = tokio::time::timeout(Duration::from_secs(5), stdout.next_line())
            .await
            .expect("an answer in time")
            .unwrap()
            .unwrap();
        assert_eq!(line, format!("got {{\"n\":{i}}}"));
    }
    drop(stdin); // EOF → stdin/close

    assert_eq!(stdout.next_line().await.unwrap(), None);
    assert_eq!(stderr.next_line().await.unwrap().as_deref(), Some("bye"));
    assert_eq!(p.wait().await.unwrap(), 3);

    let id = handle.id.clone();
    tl.destroy(handle.clone()).await.expect("destroyed");
    assert!(fake.seen(|s| s.live.is_empty()));
    // Idempotent: already gone is destroyed.
    tl.destroy(handle).await.expect("destroyed again");
    assert_eq!(fake.seen(|s| s.deletes.clone()), [id.clone(), id]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_key_is_an_error_not_a_sandbox() {
    let fake = FakeTensorlake::start().await;
    let tl = Tensorlake::new(
        Options {
            api_url: fake.url.clone(),
            api_key: Some("wrong".into()),
            ..Options::default()
        },
        Limits::default(),
    );
    let e = tl.create("img").await.unwrap_err();
    assert!(e.to_string().contains("401"), "{e}");

    let tl = Tensorlake::new(
        Options {
            api_url: fake.url.clone(),
            ..Options::default()
        },
        Limits::default(),
    );
    let e = tl.create("img").await.unwrap_err();
    assert!(e.to_string().contains("API key"), "{e}");
}
