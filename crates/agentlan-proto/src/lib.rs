//! Shared contract for AgentLAN: identifiers, registry records, the AMF/1 message
//! format, node↔hub wire frames and the local IPC protocol.
//!
//! JSON naming policy (one rule for everything): struct fields are camelCase,
//! enum tags / variant names are snake_case.

pub mod amf;
pub mod ids;
pub mod ipc;
pub mod join;
pub mod records;
pub mod wire;

/// Wire protocol major version spoken by this build.
pub const PROTO_VERSION: u32 = 1;
/// AMF version stamped on outgoing messages.
pub const AMF_VERSION: &str = "1.0";
/// Maximum serialized application frame (envelope included).
pub const MAX_FRAME: usize = 1 << 20;
/// Maximum total size of a message's parts.
pub const MAX_BODY: usize = 256 * 1024;
/// Maximum size of a single `raw` part (base64 text) in v1.
pub const MAX_RAW_PART: usize = 64 * 1024;
/// Default message time-to-live.
pub const DEFAULT_TTL_HOURS: i64 = 24;

pub type Ts = chrono::DateTime<chrono::Utc>;

pub fn now() -> Ts {
    chrono::Utc::now()
}
