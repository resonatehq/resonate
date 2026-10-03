//! Frames: newline-delimited JSON on rn8's stdin and stdout.
//!
//! ```text
//! plugin → rn8
//! {"type":"task","v":1,"task":{...}}              first frame, exactly once
//! {"type":"res","id":7,"status":200,"body":{...}}
//!
//! rn8 → plugin
//! {"type":"req","id":7,"body":{...}}              a protocol request from the SDK
//! {"type":"log","stream":"stdout","data":"..."}   worker output
//! ```
//!
//! `id` correlates a `res` with its `req`. Requests can be concurrent and
//! responses can arrive in any order, so nothing here assumes either. `body` is
//! the Resonate protocol message, unmodified: the relay is a pipe, and what
//! flows through it is the protocol's business.
//!
//! One frame per line, which is why a frame is never pretty-printed: JSON
//! escapes a newline inside a string, so the only raw `\n` in a serialized
//! frame is the one that ends it.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The frame protocol version, `v` in the task frame.
///
/// Carried once, in the first frame, because that is the only place it can be
/// checked before anything depends on it. A guest whose rn8 speaks another
/// version exits 2 before it starts the worker.
pub const VERSION: u32 = 1;

/// The longest line either side will read, in bytes.
///
/// A promise's value travels inside a frame, so this is generous. It exists so
/// that a guest writing a line without end cannot make the host buffer it
/// without end.
pub const MAX_FRAME: usize = 16 * 1024 * 1024;

/// A frame the plugin sends to rn8.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ToGuest {
    /// The task message, exactly as the server emitted it. First, and once.
    Task { v: u32, task: Value },
    /// The server's answer to the `req` with the same `id`.
    Res { id: u64, status: u16, body: Value },
}

/// A frame rn8 sends to the plugin.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum FromGuest {
    /// A protocol request the SDK made of rn8's loopback port.
    Req { id: u64, body: Value },
    /// A line the worker wrote.
    Log { stream: LogStream, data: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogStream {
    Stdout,
    Stderr,
}

#[derive(Debug)]
pub enum FrameError {
    Io(std::io::Error),
    /// A line that is not a frame of the expected direction.
    Malformed(String),
    /// A line longer than [`MAX_FRAME`].
    TooLong,
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Io(e) => write!(f, "frame i/o: {e}"),
            FrameError::Malformed(e) => write!(f, "malformed frame: {e}"),
            FrameError::TooLong => write!(f, "frame longer than {MAX_FRAME} bytes"),
        }
    }
}

impl std::error::Error for FrameError {}

impl From<std::io::Error> for FrameError {
    fn from(e: std::io::Error) -> Self {
        FrameError::Io(e)
    }
}

/// Serialize one frame, newline included.
pub fn encode<T: Serialize>(frame: &T) -> Vec<u8> {
    // A frame is an enum of owned JSON values and strings: there is no map
    // with non-string keys in it and nothing that can fail to serialize.
    let mut line = serde_json::to_vec(frame).expect("a frame always serializes");
    line.push(b'\n');
    line
}

/// Write one frame and flush it.
///
/// Flushed every time, because the other end is waiting on it: a `req` sitting
/// in a buffer is an SDK call that never returns.
pub async fn write<W, T>(w: &mut W, frame: &T) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
    T: Serialize,
{
    w.write_all(&encode(frame)).await?;
    w.flush().await
}

/// Reads frames, one per line, from a buffered reader.
pub struct FrameReader<R> {
    inner: R,
    line: Vec<u8>,
}

impl<R: AsyncBufRead + Unpin> FrameReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            line: Vec::new(),
        }
    }

    /// The next frame, or `None` at end of stream.
    ///
    /// A blank line is skipped rather than refused: it carries nothing, and a
    /// trailing newline from a hand-written test or a shell is not worth a
    /// framing error. A partial line at end of stream is malformed — the writer
    /// died mid-frame.
    ///
    /// Cancel safe, so it can be a `select!` branch: bytes of a line read
    /// before the future is dropped stay in the buffer, and the next call
    /// carries on from them. The buffer is cleared only once a line has been
    /// consumed.
    pub async fn next<T: for<'de> Deserialize<'de>>(&mut self) -> Result<Option<T>, FrameError> {
        loop {
            let room = (MAX_FRAME + 1).saturating_sub(self.line.len()) as u64;
            let n = AsyncReadExt::take(&mut self.inner, room)
                .read_until(b'\n', &mut self.line)
                .await?;
            if self.line.last() != Some(&b'\n') {
                if self.line.len() > MAX_FRAME {
                    return Err(FrameError::TooLong);
                }
                if n == 0 && self.line.is_empty() {
                    return Ok(None);
                }
                if n == 0 {
                    return Err(FrameError::Malformed(
                        "unterminated frame at end of stream".into(),
                    ));
                }
                continue;
            }
            let parsed = match trim(&self.line) {
                [] => None,
                body => Some(
                    serde_json::from_slice(body).map_err(|e| FrameError::Malformed(e.to_string())),
                ),
            };
            self.line.clear();
            match parsed {
                None => continue,
                Some(frame) => return frame.map(Some),
            }
        }
    }
}

fn trim(line: &[u8]) -> &[u8] {
    let mut end = line.len();
    while end > 0 && matches!(line[end - 1], b'\n' | b'\r' | b' ' | b'\t') {
        end -= 1;
    }
    &line[..end]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn frames_have_the_documented_shape() {
        let task = ToGuest::Task {
            v: VERSION,
            task: json!({"kind": "execute"}),
        };
        assert_eq!(
            serde_json::to_value(&task).unwrap(),
            json!({"type": "task", "v": 1, "task": {"kind": "execute"}})
        );
        let res = ToGuest::Res {
            id: 7,
            status: 200,
            body: json!({}),
        };
        assert_eq!(
            serde_json::to_value(&res).unwrap(),
            json!({"type": "res", "id": 7, "status": 200, "body": {}})
        );
        let req = FromGuest::Req {
            id: 7,
            body: json!({}),
        };
        assert_eq!(
            serde_json::to_value(&req).unwrap(),
            json!({"type": "req", "id": 7, "body": {}})
        );
        let log = FromGuest::Log {
            stream: LogStream::Stdout,
            data: "hi".into(),
        };
        assert_eq!(
            serde_json::to_value(&log).unwrap(),
            json!({"type": "log", "stream": "stdout", "data": "hi"})
        );
    }

    #[test]
    fn an_encoded_frame_is_one_line() {
        let log = FromGuest::Log {
            stream: LogStream::Stderr,
            data: "a\nb\n".into(),
        };
        let line = encode(&log);
        assert_eq!(line.iter().filter(|b| **b == b'\n').count(), 1);
        assert_eq!(line.last(), Some(&b'\n'));
    }

    #[tokio::test]
    async fn reads_frames_until_end_of_stream() {
        let input = b"{\"type\":\"res\",\"id\":1,\"status\":200,\"body\":null}\n\n{\"type\":\"res\",\"id\":2,\"status\":404,\"body\":{}}\n";
        let mut r = FrameReader::new(&input[..]);
        let a: ToGuest = r.next().await.unwrap().unwrap();
        let b: ToGuest = r.next().await.unwrap().unwrap();
        assert!(matches!(
            a,
            ToGuest::Res {
                id: 1,
                status: 200,
                ..
            }
        ));
        assert!(matches!(
            b,
            ToGuest::Res {
                id: 2,
                status: 404,
                ..
            }
        ));
        assert!(r.next::<ToGuest>().await.unwrap().is_none());
    }

    /// A `select!` that drops `next` mid-line must not lose the bytes it read.
    #[tokio::test]
    async fn a_dropped_read_loses_nothing() {
        let (mut w, r) = tokio::io::duplex(64);
        let mut frames = FrameReader::new(tokio::io::BufReader::new(r));
        w.write_all(b"{\"type\":\"res\",\"id\":9,").await.unwrap();
        // Half a frame is there; the read is abandoned before the rest comes.
        let abandoned = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            frames.next::<ToGuest>(),
        )
        .await;
        assert!(abandoned.is_err(), "nothing complete to read yet");
        w.write_all(b"\"status\":200,\"body\":{}}\n").await.unwrap();
        let f: ToGuest = frames.next().await.unwrap().unwrap();
        assert!(matches!(
            f,
            ToGuest::Res {
                id: 9,
                status: 200,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn refuses_what_is_not_a_frame() {
        for input in [
            &b"not json\n"[..],
            b"{\"type\":\"req\",\"id\":1,\"body\":{}}\n", // wrong direction
            b"{\"type\":\"res\",\"id\":1}",               // unterminated
        ] {
            let mut r = FrameReader::new(input);
            assert!(r.next::<ToGuest>().await.is_err(), "{input:?}");
        }
    }
}
