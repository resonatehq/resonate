package resonate

// ProtocolVersion is the durable-promise wire protocol version, not the SDK
// semver. It is sent in every request envelope's head.version field so the
// server can reject or coerce requests from incompatible clients. Callers
// should treat this as read-only; changing it without a matching server upgrade
// will cause request failures.
const ProtocolVersion = "2026-04-01"
