//! Reading what happened on this PC (`agentlan log`, the dashboard): the node's activity
//! log, and on the server PC the hub's view of traffic between PCs. Read-only: opens the
//! databases without migrating them, so it works next to a running node or hub.

use std::path::Path;

use agentlan_store::rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::{Value, json};

/// One thing that happened. `kind`: sent, received, state, live, session, wake, pause,
/// resume, node (this PC); message, delivery, roster (the hub's network view). `body` holds
/// the details; peer-written strings in it are untrusted.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityRow {
    pub seq: i64,
    pub ts: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub body: Value,
    /// The message text, when asked for and the message hasn't expired.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct LogFilter {
    /// Only rows after this sequence number (for following).
    pub after_seq: i64,
    /// Only rows at or after this time (RFC 3339).
    pub since: Option<String>,
    /// Only rows about this agent: a name, `name@machine` or `agt_…`.
    pub agent: Option<String>,
    pub thread: Option<String>,
    /// At most this many rows: the newest ones, oldest first.
    pub limit: usize,
    /// Attach message text.
    pub with_text: bool,
}

fn open_ro(path: &Path) -> Result<Connection> {
    if !path.exists() {
        bail!("{} doesn't exist yet", path.display());
    }
    let c = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX)
        .or_else(|_| Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX))
        .with_context(|| format!("opening {}", path.display()))?;
    c.busy_timeout(std::time::Duration::from_secs(3))?;
    Ok(c)
}

fn has_table(c: &Connection, t: &str) -> Result<bool> {
    Ok(c.query_row("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1", params![t], |_| Ok(())).optional()?.is_some())
}

/// Does this row concern `who` (a name, `name@machine` or `agt_…`)?
pub fn mentions(row: &ActivityRow, who: &str) -> bool {
    let hit = |v: &Value| v.as_str().is_some_and(|s| s == who || s.split('@').next() == Some(who) || (who.contains('@') && s == who));
    row.agent.as_deref() == Some(who)
        || ["handle", "from", "to_handle"].iter().any(|k| hit(&row.body[*k]))
        || hit(&row.body["to"]["handle"])
        || hit(&row.body["to"]["agent"])
        || row.body["localRecipients"].as_array().is_some_and(|a| a.iter().any(hit))
        || row.body["recipients"].as_array().is_some_and(|a| a.iter().any(hit))
}

/// Plain text of a stored AMF envelope's parts.
fn envelope_text(env: &str) -> Option<String> {
    let v: Value = serde_json::from_str(env).ok()?;
    let parts = v["parts"].as_array()?;
    let t: Vec<String> = parts
        .iter()
        .filter_map(|p| p["text"].as_str().map(String::from).or_else(|| p["url"].as_str().map(|u| format!("<{u}>"))).or_else(|| p.get("data").map(|d| d.to_string())))
        .collect();
    (!t.is_empty()).then(|| t.join("\n"))
}

/// This PC's activity log (newest `limit` rows matching the filter, oldest first).
pub fn read_local(home: &Path, f: &LogFilter) -> Result<Vec<ActivityRow>> {
    let c = open_ro(&crate::node_dir(home).join("node.db"))?;
    if !has_table(&c, "activity")? {
        bail!("this PC's node hasn't recorded any activity yet: it starts once the node runs AgentLAN 0.2.0-beta.6 or newer");
    }
    let since = f.since.clone().unwrap_or_default();
    // Agent filtering happens after the query (it looks into the JSON), so read a wider window.
    let window = if f.agent.is_some() { 20_000 } else { f.limit.max(1) } as i64;
    let mut st = c.prepare(
        "SELECT seq, ts, kind, agent, msg_id, thread_id, body FROM activity
         WHERE seq > ?1 AND ts >= ?2 AND (?3 IS NULL OR thread_id = ?3)
         ORDER BY seq DESC LIMIT ?4",
    )?;
    let mut rows: Vec<ActivityRow> = st
        .query_map(params![f.after_seq, since, f.thread, window], |r| {
            let body: String = r.get(6)?;
            Ok(ActivityRow {
                seq: r.get(0)?,
                ts: r.get(1)?,
                kind: r.get(2)?,
                agent: r.get(3)?,
                msg_id: r.get(4)?,
                thread_id: r.get(5)?,
                body: serde_json::from_str(&body).unwrap_or(Value::Null),
                text: None,
            })
        })?
        .collect::<Result<_, _>>()?;
    if let Some(who) = &f.agent {
        rows.retain(|r| mentions(r, who));
    }
    rows.truncate(f.limit.max(1));
    rows.reverse();
    if f.with_text {
        for r in rows.iter_mut().filter(|r| matches!(r.kind.as_str(), "sent" | "received")) {
            if let Some(m) = &r.msg_id {
                let env: Option<String> = c.query_row("SELECT envelope FROM messages WHERE msg_id=?1", params![m], |x| x.get(0)).optional()?;
                r.text = env.as_deref().and_then(envelope_text);
            }
        }
    }
    Ok(rows)
}

/// The server's view of traffic between PCs (on the server PC only): each message the
/// hub relayed, every recipient's delivery state (stored, seen, handled, expired), and
/// who came online or went offline. Sequence numbers are synthetic (time order).
pub fn read_network(home: &Path, f: &LogFilter) -> Result<Vec<ActivityRow>> {
    let path = home.join("hub").join("hub.db");
    if !path.exists() {
        bail!("this PC isn't the server: the network view is on the PC that runs `agentlan up`");
    }
    let c = open_ro(&path)?;
    let since = f.since.clone().unwrap_or_default();
    let names: std::collections::HashMap<String, String> = {
        let mut st = c.prepare("SELECT a.agent_id, a.name || '@' || m.alias FROM agents a JOIN machines m USING(machine_id)")?;
        st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?.collect::<Result<_, _>>()?
    };
    let name = |id: &str| names.get(id).cloned().unwrap_or_else(|| id.to_string());
    let mut rows = vec![];
    {
        let mut st = c.prepare("SELECT msg_id, envelope, created_at FROM messages WHERE created_at >= ?1")?;
        let msgs: Vec<(String, String, String)> = st.query_map(params![since], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?.collect::<Result<_, _>>()?;
        for (id, env, at) in msgs {
            let v: Value = serde_json::from_str(&env).unwrap_or(Value::Null);
            let thread = v["threadId"].as_str().map(String::from);
            if f.thread.is_some() && thread != f.thread {
                continue;
            }
            let mut recipients = vec![];
            let mut ds = c.prepare("SELECT recipient, state, updated FROM deliveries WHERE msg_id=?1")?;
            let dels: Vec<(String, String, String)> = ds.query_map(params![id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?.collect::<Result<_, _>>()?;
            for (rcpt, state, updated) in &dels {
                recipients.push(name(rcpt));
                rows.push(ActivityRow {
                    seq: 0,
                    ts: updated.clone(),
                    kind: "delivery".into(),
                    agent: Some(rcpt.clone()),
                    msg_id: Some(id.clone()),
                    thread_id: thread.clone(),
                    body: json!({"handle": name(rcpt), "state": state, "from": v["from"]["handle"], "subject": v["subject"], "intent": v["intent"]}),
                    text: None,
                });
            }
            rows.push(ActivityRow {
                seq: 0,
                ts: at,
                kind: "message".into(),
                agent: v["from"]["agent"].as_str().map(String::from),
                msg_id: Some(id),
                thread_id: thread,
                body: json!({"from": v["from"]["handle"], "to": v["to"], "intent": v["intent"], "subject": v["subject"], "inReplyTo": v["inReplyTo"], "recipients": recipients}),
                text: if f.with_text { envelope_text(&env) } else { None },
            });
        }
    }
    if f.thread.is_none() {
        let mut st = c.prepare("SELECT ts, body FROM events WHERE ts >= ?1")?;
        let evs: Vec<(String, String)> = st.query_map(params![since], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<Result<_, _>>()?;
        for (ts, body) in evs {
            let mut b: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
            // Profile and hardware refreshes on every reconnect are noise here.
            if matches!(b["kind"].as_str(), Some("updated" | "machine_updated")) {
                continue;
            }
            if let Some(id) = b["agentId"].as_str().or(b["agent"]["agentId"].as_str()).map(String::from) {
                b["handle"] = json!(name(&id));
            }
            rows.push(ActivityRow { seq: 0, ts, kind: "roster".into(), agent: None, msg_id: None, thread_id: None, body: b, text: None });
        }
    }
    // Time order; a message before its own delivery states.
    rows.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| (a.kind != "message").cmp(&(b.kind != "message"))));
    for (i, r) in rows.iter_mut().enumerate() {
        r.seq = i as i64 + 1;
    }
    if let Some(who) = &f.agent {
        rows.retain(|r| mentions(r, who));
    }
    rows.retain(|r| r.seq > f.after_seq);
    let skip = rows.len().saturating_sub(f.limit.max(1));
    Ok(rows.split_off(skip))
}
