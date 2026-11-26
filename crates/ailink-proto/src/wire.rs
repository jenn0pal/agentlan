//! Node ↔ hub frames (JSON inside Noise transport messages).

use serde::{Deserialize, Serialize};

use crate::amf::Amf;
use crate::records::{AgentProfile, AgentRecord, EndReason, MachineRecord, MachineReported};
use crate::Ts;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AttachedSession {
    pub agent_id: String,
    pub session_id: String,
    pub since: Ts,
}

/// Lifecycle record for a session that started and/or ended while the hub was unreachable.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SessionLifecycle {
    pub agent_id: String,
    pub session_id: String,
    pub started: Ts,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended: Option<Ts>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<EndReason>,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    Accepted,
    Stored,
    Surfaced,
    Acked,
    Expired,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JournalEntry {
    pub msg_id: String,
    pub thread_id: String,
    pub from: String,
    pub recipients: Vec<String>,
    pub intent: String,
    pub bytes: u64,
    pub created_at: Ts,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "t", rename_all = "snake_case", rename_all_fields = "camelCase")]
pub enum NodeToHub {
    Hello { protos: Vec<u32>, app_ver: String },
    Enroll { token: String, alias: String },
    MachineInfo { reported: MachineReported },
    Resync { profiles: Vec<AgentProfile>, attached: Vec<AttachedSession>, lifecycle: Vec<SessionLifecycle>, last_seq: u64 },
    ProfileUpsert { profile: AgentProfile },
    AgentUp { session_id: String, profile: AgentProfile },
    AgentDown { agent_id: String, session_id: String, fence: u64, reason: EndReason },
    /// `fence: None` = durable replay from the outbox (node-authenticated).
    Submit { agent_id: String, session_id: Option<String>, fence: Option<u64>, envelope: Amf, local_recipients: Vec<String> },
    Receipt { msg_id: String, recipient: String, state: DeliveryState },
    Journal { entry: JournalEntry },
    Ping { n: u64 },
    Pong { n: u64 },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SessionOutcome {
    pub agent_id: String,
    pub session_id: String,
    /// continued | registered | closed | rejected
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Joined,
    Left,
    Lost,
    Updated,
    Retired,
    MachineOnline,
    MachineLost,
    MachineUpdated,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RegistryEvent {
    pub seq: u64,
    pub kind: EventKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<AgentRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<MachineRecord>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "t", rename_all = "snake_case", rename_all_fields = "camelCase")]
pub enum HubToNode {
    Welcome { proto: u32, hub_id: String, machine_id: String, alias: String, conn_gen: u64, heartbeat_ms: u64, lease_ms: u64, epoch: u64 },
    Enrolled { machine_id: String, alias: String },
    ResyncAck { outcomes: Vec<SessionOutcome> },
    AgentUpAck { agent_id: String, session_id: String, fence: u64 },
    Snapshot { seq: u64, machines: Vec<MachineRecord>, agents: Vec<AgentRecord>, more: bool },
    Event { event: RegistryEvent },
    Accepted { msg_id: String, remote: u32 },
    Rejected { msg_id: String, code: String, msg: String },
    Deliver { recipient: String, pair_seq: u64, envelope: Amf },
    JournalAck { msg_id: String },
    /// The hub recorded a receipt; the node may drop its retained copy.
    ReceiptAck { msg_id: String, recipient: String, state: DeliveryState },
    Ping { n: u64 },
    Pong { n: u64 },
    Error { code: String, msg: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags() {
        let f = NodeToHub::Ping { n: 3 };
        assert_eq!(serde_json::to_string(&f).unwrap(), r#"{"t":"ping","n":3}"#);
        let f = HubToNode::JournalAck { msg_id: "m".into() };
        assert_eq!(serde_json::to_string(&f).unwrap(), r#"{"t":"journal_ack","msgId":"m"}"#);
    }
}
