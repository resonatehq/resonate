//! The owner directory from the consumer group itself.
//!
//! The group coordinator knows every member and every assignment; the Admin
//! API's `DescribeConsumerGroups` tells. A background thread asks every
//! `refresh` (and at once after [`Directory::stale`]) and replaces the whole
//! map with the answer: each member's assigned partitions of the promise
//! topic, mapped to the node and peer URL its `client.id` carries.
//!
//! What it reports is the *assignment*. A partition just assigned may still be
//! replaying at its new owner, which then answers 503 for a moment; a member
//! that died stays listed until the group's session timeout drops it. Both are
//! routing staleness the retry covers, never a safety question.
//!
//! The safe `rdkafka` API does not wrap `DescribeConsumerGroups` yet, so the
//! call is made through `rdkafka-sys` here — librdkafka has had it since 2.0.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;

use rdkafka::bindings as rd;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::ClientConfig;

use super::{decode_client_id, Directory, Owner};

/// One member of the group, as the coordinator describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub client_id: String,
    pub host: String,
    /// `(topic, partition)` pairs assigned to it.
    pub assignment: Vec<(String, i32)>,
}

/// Describe group `group` through `client`'s connection.
pub fn describe_group(
    client: &BaseConsumer,
    group: &str,
    timeout: Duration,
) -> Result<Vec<Member>, String> {
    let rk = client.client().native_ptr();
    let group_c = CString::new(group).map_err(|e| e.to_string())?;
    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;

    // SAFETY: every pointer below comes from librdkafka and is used only
    // within its lifetime: options and queue are destroyed on every path, the
    // event (which owns the result and everything reachable from it) is
    // destroyed after the members are copied out, and `group_c` outlives the
    // call that reads it.
    unsafe {
        let options = rd::rd_kafka_AdminOptions_new(
            rk,
            rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS,
        );
        if options.is_null() {
            return Err("cannot create admin options".into());
        }
        let mut errstr = [0 as c_char; 256];
        rd::rd_kafka_AdminOptions_set_request_timeout(
            options,
            timeout_ms,
            errstr.as_mut_ptr(),
            errstr.len(),
        );
        let queue = rd::rd_kafka_queue_new(rk);
        let mut groups = [group_c.as_ptr()];
        rd::rd_kafka_DescribeConsumerGroups(rk, groups.as_mut_ptr(), 1, options, queue);
        let event = rd::rd_kafka_queue_poll(queue, timeout_ms.saturating_add(1_000));
        rd::rd_kafka_AdminOptions_destroy(options);
        rd::rd_kafka_queue_destroy(queue);
        if event.is_null() {
            return Err(format!("describing group {group} timed out"));
        }
        let out = read_event(event, group);
        rd::rd_kafka_event_destroy(event);
        out
    }
}

/// # Safety
/// `event` must be a live `DescribeConsumerGroups` result event.
unsafe fn read_event(event: *mut rd::rd_kafka_event_t, group: &str) -> Result<Vec<Member>, String> {
    let string = |p: *const c_char| {
        if p.is_null() {
            String::new()
        } else {
            CStr::from_ptr(p).to_string_lossy().into_owned()
        }
    };
    if rd::rd_kafka_event_error(event) != rd::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        return Err(string(rd::rd_kafka_event_error_string(event)));
    }
    let result = rd::rd_kafka_event_DescribeConsumerGroups_result(event);
    if result.is_null() {
        return Err("not a describe result".into());
    }
    let mut count = 0usize;
    let descriptions = rd::rd_kafka_DescribeConsumerGroups_result_groups(result, &mut count);
    for g in 0..count {
        let desc = *descriptions.add(g);
        if string(rd::rd_kafka_ConsumerGroupDescription_group_id(desc)) != group {
            continue;
        }
        let error = rd::rd_kafka_ConsumerGroupDescription_error(desc);
        if !error.is_null() {
            return Err(string(rd::rd_kafka_error_string(error)));
        }
        let mut members = Vec::new();
        for m in 0..rd::rd_kafka_ConsumerGroupDescription_member_count(desc) {
            let member = rd::rd_kafka_ConsumerGroupDescription_member(desc, m);
            let mut assignment = Vec::new();
            let a = rd::rd_kafka_MemberDescription_assignment(member);
            if !a.is_null() {
                let list = rd::rd_kafka_MemberAssignment_partitions(a);
                if !list.is_null() {
                    for i in 0..(*list).cnt.max(0) as usize {
                        let tp = &*(*list).elems.add(i);
                        assignment.push((string(tp.topic), tp.partition));
                    }
                }
            }
            members.push(Member {
                client_id: string(rd::rd_kafka_MemberDescription_client_id(member)),
                host: string(rd::rd_kafka_MemberDescription_host(member)),
                assignment,
            });
        }
        return Ok(members);
    }
    // A group with no members yet is not described as an error by every
    // broker; it is simply empty.
    Ok(Vec::new())
}

/// The partition-to-owner map a description implies, for `topic`.
pub fn owners_of(members: &[Member], topic: &str) -> HashMap<u32, Owner> {
    let mut out = HashMap::new();
    for member in members {
        let Some(owner) = decode_client_id(&member.client_id) else {
            continue;
        };
        for (t, p) in &member.assignment {
            if t == topic && *p >= 0 {
                out.insert(*p as u32, owner.clone());
            }
        }
    }
    out
}

/// Settings for [`GroupDirectory`].
#[derive(Debug, Clone)]
pub struct GroupDirectoryCfg {
    pub brokers: String,
    pub group_id: String,
    /// The promise topic: the one the group subscribes to.
    pub topic: String,
    pub refresh: Duration,
    pub timeout: Duration,
    pub properties: std::collections::BTreeMap<String, String>,
}

#[derive(Default)]
struct Wake {
    now: Mutex<bool>,
    cv: Condvar,
}

/// The directory, refreshed from the group coordinator in the background.
pub struct GroupDirectory {
    owners: Arc<RwLock<HashMap<u32, Owner>>>,
    wake: Arc<Wake>,
    stop: Arc<AtomicBool>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl GroupDirectory {
    pub fn start(cfg: GroupDirectoryCfg) -> Result<Arc<Self>, String> {
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", &cfg.brokers)
            .set("group.id", format!("{}-directory", cfg.group_id));
        for (k, v) in &cfg.properties {
            c.set(k, v);
        }
        // A handle for admin calls only: it never subscribes or joins.
        let client: BaseConsumer = c.create().map_err(|e| e.to_string())?;

        let owners = Arc::new(RwLock::new(HashMap::new()));
        let wake = Arc::new(Wake::default());
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let (owners, wake, stop) = (Arc::clone(&owners), Arc::clone(&wake), Arc::clone(&stop));
            std::thread::Builder::new()
                .name("resonate-directory".into())
                .spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        match describe_group(&client, &cfg.group_id, cfg.timeout) {
                            Ok(members) => {
                                *owners.write().unwrap_or_else(|e| e.into_inner()) =
                                    owners_of(&members, &cfg.topic);
                            }
                            Err(e) => tracing::debug!(error = %e, "Group not described; keeping the last answer"),
                        }
                        let mut now = wake.now.lock().unwrap_or_else(|e| e.into_inner());
                        if !*now {
                            now = wake
                                .cv
                                .wait_timeout(now, cfg.refresh)
                                .unwrap_or_else(|e| e.into_inner())
                                .0;
                        }
                        *now = false;
                    }
                })
                .map_err(|e| e.to_string())?
        };
        Ok(Arc::new(Self {
            owners,
            wake,
            stop,
            thread: Mutex::new(Some(thread)),
        }))
    }
}

impl Directory for GroupDirectory {
    fn owner(&self, partition: u32) -> Option<Owner> {
        self.owners
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&partition)
            .cloned()
    }

    fn stale(&self) {
        *self.wake.now.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.wake.cv.notify_one();
    }
}

impl Drop for GroupDirectory {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.stale();
        if let Some(t) = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = t.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory::encode_client_id;

    #[test]
    fn only_this_topic_and_only_our_nodes_count() {
        let members = vec![
            Member {
                client_id: encode_client_id("a", "http://a:8002"),
                host: "/10.0.0.1".into(),
                assignment: vec![("r.promises".into(), 0), ("other".into(), 1)],
            },
            Member {
                client_id: encode_client_id("b", "http://b:8002"),
                host: "/10.0.0.2".into(),
                assignment: vec![("r.promises".into(), 1)],
            },
            Member {
                client_id: "someone-else".into(),
                host: "/10.0.0.3".into(),
                assignment: vec![("r.promises".into(), 2)],
            },
        ];
        let owners = owners_of(&members, "r.promises");
        assert_eq!(owners.len(), 2);
        assert_eq!(owners[&0].node, "a");
        assert_eq!(owners[&1].peer_url, "http://b:8002");
    }
}
