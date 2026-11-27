//! AMF/1 — the AgentLAN Message Format. Parts use the A2A 1.0 `Part` shape.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{AMF_VERSION, MAX_BODY, MAX_RAW_PART, Ts};

pub const INTENTS: &[&str] = &["chat", "ask", "request", "result", "status", "handoff", "error", "system"];

pub const MT_MARKDOWN: &str = "text/markdown";
pub const MT_GIT_REF: &str = "application/vnd.agentlan.git-ref+json";
pub const MT_FILE_REF: &str = "application/vnd.agentlan.file-ref+json";
pub const MT_TASK: &str = "application/vnd.agentlan.task+json";
pub const MT_STATUS: &str = "application/vnd.agentlan.status+json";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Amf {
    pub amf: String,
    pub id: String,
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    pub from: From,
    pub to: Target,
    pub intent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parts: Vec<Part>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_by: Option<Ts>,
    pub created_at: Ts,
    pub expires_at: Ts,
    /// Unknown fields are preserved (AMF rule 4: additive evolution).
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct From {
    /// Stable agent id; the only field used for routing.
    pub agent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    pub origin: Origin,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Origin {
    Agent,
    Human,
    System,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Pick {
    #[default]
    All,
    One,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", rename_all_fields = "camelCase")]
pub enum Target {
    Agent { agent: String },
    Handle { handle: String },
    Role {
        role: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        project: Option<String>,
        #[serde(default)]
        pick: Pick,
    },
    Project {
        project: String,
        #[serde(default)]
        pick: Pick,
    },
    Machine { machine: String },
    All,
}

impl Target {
    /// True for single-recipient targets (direct mail; stored for offline recipients).
    pub fn is_direct(&self) -> bool {
        matches!(self, Target::Agent { .. } | Target::Handle { .. })
    }

    /// Parse the CLI shorthand: `@name@alias`, `agt_…`, `role:R[@project]`, `project:P`, `machine:M`, `all`.
    pub fn parse(s: &str) -> Result<Target, String> {
        let s = s.trim();
        if s == "all" {
            return Ok(Target::All);
        }
        if s.starts_with("agt_") {
            return Ok(Target::Agent { agent: s.to_string() });
        }
        if let Some(r) = s.strip_prefix("role:") {
            let (role, project) = match r.split_once('@') {
                Some((a, b)) => (a, Some(b.to_string())),
                None => (r, None),
            };
            return Ok(Target::Role { role: role.to_string(), project, pick: Pick::All });
        }
        if let Some(p) = s.strip_prefix("project:") {
            return Ok(Target::Project { project: p.to_string(), pick: Pick::All });
        }
        if let Some(m) = s.strip_prefix("machine:") {
            return Ok(Target::Machine { machine: m.to_string() });
        }
        if crate::ids::split_handle(s).is_some() {
            return Ok(Target::Handle { handle: s.trim_start_matches('@').to_string() });
        }
        Err(format!("cannot parse target '{s}' (use @name@machine, agt_…, role:R, project:P, machine:M or all)"))
    }
}

/// A2A 1.0 Part: exactly one of `text` | `raw` | `url` | `data`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Part {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// base64 bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

impl Part {
    pub fn text(t: impl Into<String>) -> Part {
        Part { text: Some(t.into()), media_type: Some(MT_MARKDOWN.into()), ..Default::default() }
    }

    pub fn validate(&self) -> Result<(), String> {
        let n = [self.text.is_some(), self.raw.is_some(), self.url.is_some(), self.data.is_some()]
            .iter()
            .filter(|b| **b)
            .count();
        if n != 1 {
            return Err("a part must contain exactly one of text, raw, url, data".into());
        }
        if self.raw.as_ref().is_some_and(|r| r.len() > MAX_RAW_PART) {
            return Err(format!("raw part exceeds {MAX_RAW_PART} bytes"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AmfError {
    Invalid(String),
}

impl std::fmt::Display for AmfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AmfError::Invalid(s) => write!(f, "invalid AMF message: {s}"),
        }
    }
}
impl std::error::Error for AmfError {}

/// Longest message, thread, reply or task id accepted.
pub const MAX_ID_LEN: usize = 256;

pub fn intent_ok(i: &str) -> bool {
    INTENTS.contains(&i) || (i.starts_with("x-") && i.len() > 2)
}

/// Is this message (AMF JSON) an answer that needs no further handling once the asker has
/// it? A reply (`inReplyTo` set) with intent `result`, `status` or `error`; and, when
/// `matched` (the node itself matched it to the caller's request, as `request` does), also
/// `chat`, which is what a plain `send --reply-to` uses. An unmatched `chat` could be a
/// follow-up question, so it stays open. `request` and `wait_for_message` mark answers
/// handled when they return them; asks, requests, handoffs, custom intents and
/// non-replies stay open until the agent replies or acks.
pub fn is_answer(m: &Value, matched: bool) -> bool {
    m["inReplyTo"].as_str().is_some_and(|s| !s.is_empty())
        && match m["intent"].as_str() {
            Some("result" | "status" | "error") => true,
            Some("chat") => matched,
            _ => false,
        }
}

#[cfg(test)]
mod answer_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn answers_are_replies_that_ask_nothing_back() {
        for i in ["result", "status", "error"] {
            assert!(is_answer(&json!({"intent": i, "inReplyTo": "msg_1"}), false), "{i}");
        }
        assert!(is_answer(&json!({"intent": "chat", "inReplyTo": "msg_1"}), true), "a matched chat reply");
        assert!(!is_answer(&json!({"intent": "chat", "inReplyTo": "msg_1"}), false), "an unmatched chat may be a follow-up question");
        for i in ["ask", "request", "handoff", "x-review"] {
            assert!(!is_answer(&json!({"intent": i, "inReplyTo": "msg_1"}), true), "{i}");
        }
        assert!(!is_answer(&json!({"intent": "result"}), true), "not a reply");
        assert!(!is_answer(&json!({"intent": "result", "inReplyTo": ""}), true), "empty inReplyTo");
    }
}

impl Amf {
    /// Validate the rules every node enforces before accepting a message.
    pub fn validate(&self) -> Result<(), AmfError> {
        let bad = |s: String| Err(AmfError::Invalid(s));
        if !self.amf.starts_with("1.") {
            return bad(format!("unsupported amf version {}", self.amf));
        }
        if !intent_ok(&self.intent) {
            return bad(format!("unknown intent '{}' (custom intents use x-<name>)", self.intent));
        }
        // Ids are references, not content: bounded, so a reply can always carry them and
        // viewers can put them in a URL.
        for (what, v) in [("id", Some(&self.id)), ("threadId", Some(&self.thread_id)), ("inReplyTo", self.in_reply_to.as_ref()), ("taskId", self.task_id.as_ref())] {
            if v.is_some_and(|v| v.len() > MAX_ID_LEN) {
                return bad(format!("{what} is longer than {MAX_ID_LEN} bytes"));
            }
        }
        for p in &self.parts {
            p.validate().map_err(AmfError::Invalid)?;
        }
        let has_subject = self.subject.as_deref().is_some_and(|s| !s.trim().is_empty());
        let has_text = self.parts.iter().any(|p| p.text.as_deref().is_some_and(|t| !t.trim().is_empty()));
        if !has_subject && !has_text {
            return bad("plain-text floor: a message needs a subject or a text part".into());
        }
        let body: usize = self
            .parts
            .iter()
            .map(|p| serde_json::to_vec(p).map(|v| v.len()).unwrap_or(usize::MAX))
            .sum::<usize>()
            + self.subject.as_ref().map(|s| s.len()).unwrap_or(0)
            + self.extra.iter().map(|(k, v)| k.len() + v.to_string().len()).sum::<usize>();
        if body > MAX_BODY {
            return bad(format!("TOO_LARGE: message content is {body} bytes; the limit is {MAX_BODY}"));
        }
        Ok(())
    }

    /// Plain-text rendering usable by any agent without a translator.
    pub fn render_text(&self) -> String {
        let mut s = String::new();
        if let Some(sub) = &self.subject {
            s.push_str(sub);
            s.push('\n');
        }
        for p in &self.parts {
            if let Some(t) = &p.text {
                s.push_str(t);
            } else {
                s.push_str(&format!(
                    "[{}{}]",
                    p.media_type.as_deref().unwrap_or("attachment"),
                    p.filename.as_deref().map(|f| format!(" {f}")).unwrap_or_default()
                ));
            }
            s.push('\n');
        }
        s
    }
}

/// What a client asks the node to send. The node stamps `id` (unless given for
/// idempotent retries), `from`, `createdAt`, `expiresAt` and `amf`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct SendDraft {
    /// Optional client-chosen message id: retries with the same id are deduplicated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub to: Option<Target>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parts: Option<Vec<Part>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_by: Option<Ts>,
}

impl SendDraft {
    pub fn into_amf(self, from: From, default_ttl_hours: i64) -> Result<Amf, AmfError> {
        if self.text.is_some() && self.parts.is_some() {
            return Err(AmfError::Invalid("give exactly one of text or parts".into()));
        }
        let to = self.to.ok_or_else(|| AmfError::Invalid("missing 'to'".into()))?;
        let parts = match (self.text, self.parts) {
            (Some(t), None) => vec![Part::text(t)],
            (None, Some(p)) => p,
            _ => vec![],
        };
        let created = crate::now();
        let m = Amf {
            amf: AMF_VERSION.into(),
            id: self.id.unwrap_or_else(crate::ids::msg_id),
            thread_id: self.thread_id.unwrap_or_else(crate::ids::thread_id),
            task_id: None,
            in_reply_to: self.in_reply_to,
            from,
            to,
            intent: self.intent.unwrap_or_else(|| "chat".into()),
            subject: self.subject,
            parts,
            reply_by: self.reply_by,
            created_at: created,
            expires_at: created + chrono::Duration::hours(default_ttl_hours),
            extra: Map::new(),
        };
        m.validate()?;
        Ok(m)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn from() -> From {
        From { agent: "agt_X".into(), handle: Some("a@pc".into()), role: Some("qa".into()), origin: Origin::Agent }
    }

    #[test]
    fn roundtrip_and_unknown_fields() {
        let d = SendDraft { to: Some(Target::parse("role:reviewer@shop").unwrap()), text: Some("hi".into()), ..Default::default() };
        let m = d.into_amf(from(), 24).unwrap();
        let mut v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["to"]["kind"], "role");
        assert!(v.get("threadId").is_some());
        v["futureField"] = serde_json::json!(42);
        let back: Amf = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(serde_json::to_value(&back).unwrap()["futureField"], 42);
    }

    #[test]
    fn rules() {
        let empty = SendDraft { to: Some(Target::All), ..Default::default() };
        assert!(empty.into_amf(from(), 24).is_err(), "plain-text floor");
        let both = SendDraft { to: Some(Target::All), text: Some("a".into()), parts: Some(vec![]), ..Default::default() };
        assert!(both.into_amf(from(), 24).is_err(), "text xor parts");
        let bad_part = Part { text: Some("a".into()), url: Some("u".into()), ..Default::default() };
        assert!(bad_part.validate().is_err());
        let bad_intent = SendDraft { to: Some(Target::All), text: Some("a".into()), intent: Some("ack".into()), ..Default::default() };
        assert!(bad_intent.into_amf(from(), 24).is_err(), "ack is transport-level, not an intent");
        let long_thread = SendDraft { to: Some(Target::All), text: Some("a".into()), thread_id: Some("t".repeat(MAX_ID_LEN + 1)), ..Default::default() };
        assert!(long_thread.into_amf(from(), 24).is_err(), "ids are bounded");
        let ok_thread = SendDraft { to: Some(Target::All), text: Some("a".into()), thread_id: Some("t".repeat(MAX_ID_LEN)), ..Default::default() };
        assert!(ok_thread.into_amf(from(), 24).is_ok());
        assert!(Target::parse("@qa@studio").unwrap().is_direct());
    }
}
