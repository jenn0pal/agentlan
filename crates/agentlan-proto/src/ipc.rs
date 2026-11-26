//! Local IPC between clients (CLI, MCP adapter, scripts) and the node daemon.
//! Transport: JSON lines over a Unix socket (macOS/Linux) or named pipe (Windows).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::amf::SendDraft;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IpcRequest {
    pub id: u64,
    #[serde(flatten)]
    pub op: IpcOp,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", rename_all_fields = "camelCase")]
pub enum IpcOp {
    /// Bind this connection to an agent session until it closes or detaches.
    Attach {
        agent: String,
        #[serde(default)]
        runner: Option<String>,
        #[serde(default)]
        pid: Option<u32>,
        #[serde(default)]
        takeover: bool,
    },
    Detach,
    Whoami,
    Status,
    ListAgents {
        #[serde(default)]
        project: Option<String>,
        #[serde(default)]
        role: Option<String>,
    },
    ListMachines,
    Send {
        /// Sending agent (name or id). Defaults to the attached agent.
        #[serde(default, rename = "as")]
        as_agent: Option<String>,
        draft: SendDraft,
    },
    Inbox {
        #[serde(default)]
        agent: Option<String>,
        #[serde(default)]
        limit: Option<u32>,
        #[serde(default)]
        message_id: Option<String>,
    },
    Ack {
        #[serde(default)]
        agent: Option<String>,
        message_id: String,
    },
    /// Long-poll: resolves with the next unseen message (or times out). With
    /// `inReplyTo`, resolves only with a reply to that message and leaves other mail alone.
    Wait {
        #[serde(default)]
        agent: Option<String>,
        timeout_ms: u64,
        #[serde(default)]
        in_reply_to: Option<String>,
    },
    /// Drop this connection's pending waits (the caller gave up).
    CancelWait,
    /// Put a message back to "unseen" (a wait result that never reached its caller).
    Unsurface {
        #[serde(default)]
        agent: Option<String>,
        message_id: String,
    },
    /// Subscribe this connection to pushed events (messages + roster changes).
    Watch {
        #[serde(default)]
        agent: Option<String>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IpcError {
    pub code: String,
    pub msg: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IpcResponse {
    pub id: u64,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<IpcError>,
}

/// Unsolicited event pushed to watching / attached connections.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IpcPush {
    /// "message" | "roster" | "session" | "hub"
    pub event: String,
    pub data: Value,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum IpcOut {
    Response(IpcResponse),
    Push(IpcPush),
}

/// Result of a send, reported separately for the local and remote legs.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SendResult {
    pub id: String,
    pub thread_id: String,
    pub local: u32,
    pub remote: RemoteResult,
    /// True when this id had already been accepted (idempotent retry).
    #[serde(default)]
    pub duplicate: bool,
    /// The caller's retry key, if one was given (the wire id is derived from it per sender).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case", rename_all_fields = "camelCase")]
pub enum RemoteResult {
    /// No remote leg was needed.
    None,
    Accepted { count: u32 },
    /// Hub unreachable or slow: committed to the outbox, will be flushed later.
    Queued,
    Rejected { code: String, msg: String },
}
