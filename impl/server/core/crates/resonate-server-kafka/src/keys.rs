//! Where things live: which partition owns an id, and how ids are laid out in
//! the local store.
//!
//! # Contract
//!
//! - [`origin_of`] is the routing key. Every promise and task operation is
//!   single-origin, so the origin decides the partition, and one partition
//!   owner can answer any single operation on it.
//! - [`partition_of`] is Kafka's own default partitioner — murmur2 over the key
//!   bytes, masked positive, modulo the partition count — so the partition a
//!   record is produced to and the partition a request is routed to are the
//!   same function, and tools outside this process agree with both. Records are
//!   always produced to an explicit partition computed here; the producer's
//!   partitioner is never consulted.
//! - Local keys sort an origin's promises together: `o` + the origin's length +
//!   the origin + the id. Length-prefixing makes the origin prefix exact, so
//!   loading origin `a` never picks up the promises of origin `ab`.
//!
//! # Dependencies
//!
//! None.
//!
//! # Dependants
//!
//! The node routes requests with [`partition_of`]; the partition shell and the
//! local stores use the key layout.

/// The origin a promise or task id belongs to: everything before the first
/// `':'`.
pub fn origin_of(id: &str) -> &str {
    id.split_once(':').map(|(o, _)| o).unwrap_or(id)
}

/// Kafka's murmur2, bit for bit (`org.apache.kafka.common.utils.Utils`).
pub fn murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let length = data.len();
    let mut h: u32 = SEED ^ (length as u32);

    let blocks = length / 4;
    for i in 0..blocks {
        let i4 = i * 4;
        let mut k = (data[i4] as u32)
            | ((data[i4 + 1] as u32) << 8)
            | ((data[i4 + 2] as u32) << 16)
            | ((data[i4 + 3] as u32) << 24);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    let tail = blocks * 4;
    let rem = length % 4;
    if rem == 3 {
        h ^= (data[tail + 2] as u32) << 16;
    }
    if rem >= 2 {
        h ^= (data[tail + 1] as u32) << 8;
    }
    if rem >= 1 {
        h ^= data[tail] as u32;
        h = h.wrapping_mul(M);
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h as i32
}

/// The partition that owns `key` — Kafka's `toPositive(murmur2(key)) % n`.
pub fn partition_of(key: &str, partitions: u32) -> u32 {
    debug_assert!(partitions > 0, "a topic has at least one partition");
    ((murmur2(key.as_bytes()) as u32 & 0x7fff_ffff) % partitions.max(1)) as u32
}

/// The partition that owns every promise and task of `id`'s origin.
pub fn partition_of_id(id: &str, partitions: u32) -> u32 {
    partition_of(origin_of(id), partitions)
}

// ---------------------------------------------------------------------------
// Local key layout
// ---------------------------------------------------------------------------

const PROMISE: u8 = b'o';
const SCHEDULE: u8 = b's';

/// The prefix every promise of `origin` shares in the local store.
pub fn origin_prefix(origin: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(5 + origin.len());
    key.push(PROMISE);
    key.extend_from_slice(&(origin.len() as u32).to_be_bytes());
    key.extend_from_slice(origin.as_bytes());
    key
}

/// The local key of promise `id`.
pub fn promise_key(id: &str) -> Vec<u8> {
    let mut key = origin_prefix(origin_of(id));
    key.extend_from_slice(id.as_bytes());
    key
}

/// The prefix every promise shares, whatever its origin.
pub fn all_promises_prefix() -> Vec<u8> {
    vec![PROMISE]
}

/// The local key of schedule `id`.
pub fn schedule_key(id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + id.len());
    key.push(SCHEDULE);
    key.extend_from_slice(id.as_bytes());
    key
}

/// The prefix every schedule shares.
pub fn all_schedules_prefix() -> Vec<u8> {
    vec![SCHEDULE]
}

/// The promise id a local promise key names.
pub fn id_of_promise_key(key: &[u8]) -> Option<String> {
    let (&tag, rest) = key.split_first()?;
    if tag != PROMISE || rest.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes(rest[..4].try_into().ok()?) as usize;
    let id = rest.get(4 + len..)?;
    // The id carries its origin again, which is what the length skipped.
    String::from_utf8(id.to_vec()).ok()
}

/// The schedule id a local schedule key names.
pub fn id_of_schedule_key(key: &[u8]) -> Option<String> {
    let (&tag, rest) = key.split_first()?;
    if tag != SCHEDULE {
        return None;
    }
    String::from_utf8(rest.to_vec()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The vectors from Kafka's own `UtilsTest.testMurmur2`.
    #[test]
    fn murmur2_matches_kafka() {
        let cases: &[(&[u8], i32)] = &[
            (b"21", -973_932_308),
            (b"foobar", -790_332_482),
            (b"a-little-bit-long-string", -985_981_536),
            (b"a-little-bit-longer-string", -1_486_304_829),
            (
                b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8",
                -58_897_971,
            ),
            (b"abc", 479_470_107),
        ];
        for (input, want) in cases {
            assert_eq!(
                murmur2(input),
                *want,
                "{:?}",
                std::str::from_utf8(input).unwrap()
            );
        }
    }

    /// librdkafka's `murmur2_random` partitioner is the one a producer would
    /// use for a keyed record. Routing must land where it would.
    #[test]
    fn partitioning_matches_librdkafka() {
        for key in ["", "a", "order-7", "テスト", "some:longer:origin:id", "21"] {
            for n in [1u32, 3, 16, 64, 256] {
                let theirs = unsafe {
                    rdkafka::bindings::rd_kafka_msg_partitioner_murmur2(
                        std::ptr::null(),
                        key.as_ptr() as *const std::ffi::c_void,
                        key.len(),
                        n as i32,
                        std::ptr::null_mut(),
                        std::ptr::null_mut(),
                    )
                };
                assert_eq!(partition_of(key, n) as i32, theirs, "key {key:?}, n {n}");
            }
        }
    }

    #[test]
    fn an_id_is_routed_by_its_origin() {
        assert_eq!(origin_of("order-7:charge:1"), "order-7");
        assert_eq!(origin_of("order-7"), "order-7");
        assert_eq!(
            partition_of_id("order-7:charge", 64),
            partition_of("order-7", 64)
        );
    }

    #[test]
    fn an_origin_prefix_is_exact() {
        let a = origin_prefix("a");
        assert!(promise_key("a").starts_with(&a));
        assert!(promise_key("a:x").starts_with(&a));
        assert!(!promise_key("ab").starts_with(&a));
        assert!(!promise_key("ab:y").starts_with(&a));
    }

    #[test]
    fn local_keys_round_trip() {
        for id in ["a", "a:x", "order-7:charge", "テスト:1"] {
            assert_eq!(id_of_promise_key(&promise_key(id)).as_deref(), Some(id));
        }
        assert_eq!(
            id_of_schedule_key(&schedule_key("nightly")).as_deref(),
            Some("nightly")
        );
        assert_eq!(id_of_promise_key(&schedule_key("x")), None);
        assert_eq!(id_of_schedule_key(&promise_key("x")), None);
    }
}
