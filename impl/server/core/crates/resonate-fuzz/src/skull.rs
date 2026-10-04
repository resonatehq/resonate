//! Talking to skulld from inside its guest.
//!
//! Every container has the agent's socket at /run/skull/libskull.sock. One
//! SOCK_SEQPACKET packet is one message: a type byte, then the payload, and `J`
//! carries a JSON event — all libskull.so's `skull_emit` does, so this needs
//! no C library. Outside a guest there is no socket and every call is a no-op.
//!
//! Two kinds of event go out:
//!
//! * assertions, in the shape skull.h builds — a catalog entry for every site
//!   up front (so a `sometimes` that never held is reported), then each site
//!   once per condition value;
//! * `{"resonate_trace": …}` for every request and response, the same rows the
//!   Go scenarios driver streams, so `check-runs.sh` checks them on the host.

use std::collections::HashMap;
use std::os::fd::RawFd;

use serde_json::{json, Value};

const SOCKET: &str = "/run/skull/libskull.sock";

pub struct Skull {
    fd: Option<RawFd>,
    seen: HashMap<&'static str, u8>,
}

/// An assertion site: (key, assert_type, display_type, must_hit, message).
pub type Site = (&'static str, &'static str, &'static str, bool, &'static str);

pub const AGREES: Site = (
    "agrees",
    "always",
    "Always",
    true,
    "the server answers every request as the oracle does",
);
pub const STATE_AGREES: Site = (
    "state_agrees",
    "always",
    "Always",
    true,
    "after a program the server's state is the oracle's",
);
pub const OFFER_TAKEN: Site = (
    "offer_taken",
    "sometimes",
    "Sometimes",
    true,
    "an execute offer the server pushed was acquired",
);
pub const AMBIGUOUS: Site = (
    "ambiguous",
    "reachability",
    "Reachable",
    false,
    "the server gave no definite answer and the program stopped",
);
pub const FINISHED: Site = (
    "finished",
    "reachability",
    "Reachable",
    true,
    "the fuzzer finished",
);

pub const SITES: &[Site] = &[AGREES, STATE_AGREES, OFFER_TAKEN, AMBIGUOUS, FINISHED];

impl Skull {
    pub fn connect() -> Self {
        let fd = unsafe {
            let fd = libc::socket(libc::AF_UNIX, libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC, 0);
            if fd < 0 {
                None
            } else {
                let mut addr: libc::sockaddr_un = std::mem::zeroed();
                addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
                for (i, b) in SOCKET.bytes().enumerate() {
                    addr.sun_path[i] = b as libc::c_char;
                }
                let len = std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t;
                if libc::connect(fd, &addr as *const _ as *const libc::sockaddr, len) == 0 {
                    Some(fd)
                } else {
                    libc::close(fd);
                    None
                }
            }
        };
        Skull {
            fd,
            seen: HashMap::new(),
        }
    }

    pub fn active(&self) -> bool {
        self.fd.is_some()
    }

    fn emit(&self, event: &Value) {
        let Some(fd) = self.fd else { return };
        let mut packet = vec![b'J'];
        packet.extend(serde_json::to_vec(event).unwrap_or_default());
        unsafe {
            libc::send(
                fd,
                packet.as_ptr() as *const _,
                packet.len(),
                libc::MSG_NOSIGNAL,
            );
        }
    }

    fn site(site: &Site, hit: bool, cond: bool, details: Value) -> Value {
        let (key, assert_type, display_type, must_hit, message) = *site;
        json!({ "skull_assert": {
            "id": message, "message": message, "assert_type": assert_type,
            "display_type": display_type, "hit": hit, "must_hit": must_hit, "condition": cond,
            "location": { "file": "resonate-fuzz", "function": key, "class": "",
                          "begin_line": 0, "begin_column": 0 },
            "details": details,
        }})
    }

    /// Declare every site, so skulld knows what was never reached.
    pub fn catalog(&self) {
        for s in SITES {
            self.emit(&Self::site(s, false, false, Value::Null));
        }
    }

    /// Once per site per condition value, as skull.h does.
    pub fn check(&mut self, site: &Site, cond: bool, details: Value) {
        let bit = if cond { 1 } else { 2 };
        let seen = self.seen.entry(site.0).or_insert(0);
        if *seen & bit != 0 {
            return;
        }
        *seen |= bit;
        self.emit(&Self::site(site, true, cond, details));
    }

    pub fn trace(&self, row: &Value) {
        self.emit(&json!({ "resonate_trace": row }));
    }
}
