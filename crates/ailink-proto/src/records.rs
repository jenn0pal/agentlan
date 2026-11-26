//! Registry records: machines and agents.

use serde::{Deserialize, Serialize};

use crate::Ts;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum PresenceState {
    Online,
    #[default]
    Offline,
    Lost,
    Retired,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Readiness {
    Idle,
    Busy,
    #[default]
    Detached,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum AgentMode {
    #[default]
    Interactive,
    Worker,
    Script,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EndReason {
    Left,
    Detached,
    LeaseExpired,
    Takeover,
    HubRestart,
    Retired,
}

/// Metadata a node reports about its own machine (`MachineInfo`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct MachineReported {
    pub hostname: String,
    pub os: String,
    pub os_version: String,
    pub arch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_cores: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ram_gb: Option<u32>,
    #[serde(default)]
    pub addrs: Vec<String>,
    pub ailink_version: String,
}

/// Hub-owned machine record. `reported` comes from the node; everything else is hub-derived.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MachineRecord {
    pub machine_id: String,
    pub alias: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported: Option<MachineReported>,
    #[serde(default)]
    pub labels: Vec<String>,
    pub key_fp: String,
    pub state: PresenceState,
    pub agents_online: u32,
    pub enrolled_at: Ts,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<Ts>,
}

/// An agent profile as defined on its node (`profiles.toml`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentProfile {
    pub agent_id: String,
    pub name: String,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub runner: String,
    #[serde(default)]
    pub mode: AgentMode,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Network-wide agent record as held by the hub registry.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentRecord {
    #[serde(flatten)]
    pub profile: AgentProfile,
    pub machine_id: String,
    pub handle: String,
    pub state: PresenceState,
    pub readiness: Readiness,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<Ts>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_reason: Option<EndReason>,
}

/// How an agent appears to a particular viewer (adds locality and staleness).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RosterEntry {
    #[serde(flatten)]
    pub agent: AgentRecord,
    pub same_machine: bool,
    /// True when this view comes from a cache while the hub is unreachable.
    pub stale: bool,
}
