//! Server-sent events, as much of them as following output needs: `data:`
//! lines, joined, dispatched at a blank line. Event names, ids and retry hints
//! are read past.

#[derive(Default)]
pub struct Parser {
    buf: Vec<u8>,
    data: Vec<String>,
}

impl Parser {
    /// Feed bytes as they arrive; answer the data of every event completed.
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<String> {
        self.buf.extend_from_slice(bytes);
        let mut events = Vec::new();
        while let Some(end) = self.buf.iter().position(|b| *b == b'\n') {
            let raw: Vec<u8> = self.buf.drain(..=end).collect();
            let line = String::from_utf8_lossy(&raw);
            let line = line.trim_end_matches(['\n', '\r']);
            if line.is_empty() {
                if !self.data.is_empty() {
                    events.push(self.data.join("\n"));
                    self.data.clear();
                }
            } else if let Some(value) = line.strip_prefix("data:") {
                self.data
                    .push(value.strip_prefix(' ').unwrap_or(value).to_string());
            }
            // `event:`, `id:`, `retry:` and `:` comments carry nothing here.
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_end_at_a_blank_line_whatever_the_chunking() {
        let mut p = Parser::default();
        assert!(p.feed(b"data: {\"line\":").is_empty());
        assert!(p.feed(b"\"a\"}\r\n").is_empty());
        assert_eq!(
            p.feed(b"\r\n: ping\n\nevent: x\ndata:b\n\n"),
            ["{\"line\":\"a\"}", "b"]
        );
    }

    #[test]
    fn multi_line_data_is_joined() {
        let mut p = Parser::default();
        assert_eq!(p.feed(b"data: a\ndata: b\n\n"), ["a\nb"]);
    }
}
