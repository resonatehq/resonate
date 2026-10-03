//! The backend against real Tensorlake. Runs only with TENSORLAKE_API_KEY set;
//! skipped otherwise, so CI without a key stays green.
//!
//!   TENSORLAKE_API_KEY=… cargo test -p resonate-sandbox-tensorlake --test live -- --nocapture

use std::time::{Duration, Instant};

use resonate_sandbox::{Backend, Command, Egress, Limits, Process};
use resonate_sandbox_tensorlake::{Options, Tensorlake};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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
