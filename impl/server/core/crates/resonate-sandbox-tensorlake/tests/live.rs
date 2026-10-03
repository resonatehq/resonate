//! The backend against real Tensorlake. Runs only with TENSORLAKE_API_KEY set;
//! skipped otherwise, so CI without a key stays green.
//!
//!   TENSORLAKE_API_KEY=… cargo test -p resonate-sandbox-tensorlake --test live -- --nocapture

use std::time::{Duration, Instant};

use resonate_sandbox::{Backend, Command, Egress, Limits, Process};
use resonate_sandbox_tensorlake::{Options, Tensorlake};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// One test at a time: an account's sandbox quota may be a single sandbox.
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn backend() -> Option<Tensorlake> {
    let key = std::env::var("TENSORLAKE_API_KEY")
        .ok()
        .filter(|k| !k.is_empty())?;
    Some(Tensorlake::new(
        Options {
            api_key: Some(key),
            timeout_secs: 300,
            ..Options::default()
        },
        Limits {
            egress: Egress::None,
            ..Limits::default()
        },
    ))
}

#[tokio::test(flavor = "multi_thread")]
async fn live_stdio_round_trip_and_exit_status() {
    let Some(tl) = backend() else {
        eprintln!("skipped: no TENSORLAKE_API_KEY");
        return;
    };
    let _one = ONE_AT_A_TIME.lock().await;
    let image = std::env::var("TL_IMAGE").unwrap_or_else(|_| "tensorlake/ubuntu-minimal".into());

    let t = Instant::now();
    let handle = tl.create(&image).await.expect("created");
    eprintln!(
        "created {} in {:?} at {}",
        handle.id,
        t.elapsed(),
        handle.url
    );

    let result = async {
        let t = Instant::now();
        let mut p = tl
            .exec(
                &handle,
                Command {
                    argv: vec![
                        "sh".into(),
                        "-c".into(),
                        "while read l; do echo \"got $l\"; done; echo \"$GREETING\" >&2; exit 3"
                            .into(),
                    ],
                    env: vec![("GREETING".into(), "bye".into())],
                },
            )
            .await
            .expect("started");
        eprintln!("started in {:?}", t.elapsed());
        let mut stdin = p.stdin().unwrap();
        let mut stdout = BufReader::new(p.stdout().unwrap()).lines();
        let mut stderr = BufReader::new(p.stderr().unwrap()).lines();

        // Live: each answer before the next line is sent.
        for i in 0..3 {
            let t = Instant::now();
            let frame = format!(
                "{{\"type\":\"res\",\"id\":{i},\"body\":{{\"n\":\"{}\"}}}}",
                "x".repeat(i * 1000)
            );
            stdin.write_all(frame.as_bytes()).await.unwrap();
            stdin.write_all(b"\n").await.unwrap();
            stdin.flush().await.unwrap();
            let line = tokio::time::timeout(Duration::from_secs(20), stdout.next_line())
                .await
                .expect("an answer in time")
                .unwrap()
                .unwrap();
            assert_eq!(line, format!("got {frame}"));
            eprintln!(
                "round trip {i} ({} bytes) in {:?}",
                frame.len(),
                t.elapsed()
            );
        }
        drop(stdin);

        let rest = tokio::time::timeout(Duration::from_secs(20), stdout.next_line()).await;
        eprintln!("stdout after EOF: {rest:?}");
        let err = tokio::time::timeout(Duration::from_secs(20), stderr.next_line()).await;
        eprintln!("stderr: {err:?}");
        let code = tokio::time::timeout(Duration::from_secs(30), p.wait()).await;
        eprintln!("exit: {code:?}");
        assert_eq!(err.unwrap().unwrap().as_deref(), Some("bye"));
        assert_eq!(code.unwrap().unwrap(), 3);
    }
    .await;

    tl.destroy(handle.clone()).await.expect("destroyed");
    tl.destroy(handle)
        .await
        .expect("destroyed again: idempotent");
    result
}

/// An allow-list lets the guest reach what is on it and nothing else.
#[tokio::test(flavor = "multi_thread")]
async fn live_allow_list_is_enforced() {
    let Some(key) = std::env::var("TENSORLAKE_API_KEY")
        .ok()
        .filter(|k| !k.is_empty())
    else {
        eprintln!("skipped: no TENSORLAKE_API_KEY");
        return;
    };
    let _one = ONE_AT_A_TIME.lock().await;
    let tl = Tensorlake::new(
        Options {
            api_key: Some(key),
            timeout_secs: 300,
            ..Options::default()
        },
        Limits {
            egress: Egress::Allow(vec!["books.toscrape.com".into()]),
            ..Limits::default()
        },
    );
    let handle = tl
        .create("tensorlake/ubuntu-minimal")
        .await
        .expect("created");
    let probe = "import urllib.request\n\
                 for url in ['https://books.toscrape.com/', 'https://example.com/']:\n\
                 \x20   try:\n\
                 \x20       print(url, urllib.request.urlopen(url, timeout=8).status, flush=True)\n\
                 \x20   except Exception as e:\n\
                 \x20       print(url, 'blocked', type(e).__name__, flush=True)\n";
    let result = async {
        let mut p = tl
            .exec(
                &handle,
                Command {
                    argv: vec!["python3".into(), "-c".into(), probe.into()],
                    env: vec![],
                },
            )
            .await
            .expect("started");
        drop(p.stdin());
        let mut out = BufReader::new(p.stdout().unwrap()).lines();
        let mut lines = Vec::new();
        while let Ok(Ok(Some(l))) =
            tokio::time::timeout(Duration::from_secs(30), out.next_line()).await
        {
            eprintln!("{l}");
            lines.push(l);
        }
        lines
    }
    .await;
    tl.destroy(handle).await.expect("destroyed");
    assert!(
        result
            .iter()
            .any(|l| l.starts_with("https://books.toscrape.com/ 200")),
        "{result:?}"
    );
    assert!(
        result
            .iter()
            .any(|l| l.starts_with("https://example.com/ blocked")),
        "{result:?}"
    );
}
