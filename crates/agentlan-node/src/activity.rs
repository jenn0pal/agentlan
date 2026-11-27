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
    /// Read forward from `after_seq` (this PC) or `net_after` (network) instead of the
    /// newest rows.
    pub incremental: bool,
    /// This PC: only rows after this sequence number (with `incremental`).
    pub after_seq: i64,
    /// The network view: the cursor from the previous page (with `incremental`).
    pub net_after: Option<String>,
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

/// What one read returned, and where to continue from (`--follow`, the dashboard).
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Page {
    pub rows: Vec<ActivityRow>,
    /// This PC's log: the highest sequence number covered (pass as `after_seq`).
    pub cursor: i64,
    /// The network view: where to continue (pass as `net_after`). Delivery updates at the
    /// boundary time can come again, so dedupe them.
    pub net_cursor: String,
    /// The page was full: read again from the cursor before waiting.
    pub more: bool,
}

/// Longest subject and message text a reader returns.
pub(crate) const MAX_SUBJECT: usize = 300;
const MAX_TEXT: usize = 20_000;

pub(crate) fn cap(s: &str, n: usize) -> String {
    if s.chars().count() <= n { s.to_string() } else { format!("{}…", s.chars().take(n).collect::<String>()) }
}

/// Open a database read-only (no migration, no writes). A WAL database needs its shared
/// memory file, which the running node or hub keeps; if that can't be used, the first
/// read fails with a clear message.
fn open_ro(path: &Path) -> Result<Connection> {
    if !path.exists() {
        bail!("{} doesn't exist yet", path.display());
    }
    let c = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX).with_context(|| format!("opening {}", path.display()))?;
    c.busy_timeout(std::time::Duration::from_secs(3))?;
    Ok(c)
}

fn has_table(c: &Connection, t: &str) -> Result<bool> {
    c.query_row("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1", params![t], |_| Ok(()))
        .optional()
        .map(|o| o.is_some())
        .context("couldn't read the database (if the node isn't running, start it with `agentlan connect`)")
}

/// Does this row concern `who` (a name, `name@machine` or `agt_…`)?
pub fn mentions(row: &ActivityRow, who: &str) -> bool {
    let hit = |v: &Value| v.as_str().is_some_and(|s| s == who || (!who.contains('@') && s.split('@').next() == Some(who)));
    row.agent.as_deref() == Some(who)
        || ["handle", "from"].iter().any(|k| hit(&row.body[*k]))
        || hit(&row.body["to"]["handle"])
        || hit(&row.body["to"]["agent"])
        || row.body["localRecipients"].as_array().is_some_and(|a| a.iter().any(hit))
        || row.body["recipients"].as_array().is_some_and(|a| a.iter().any(hit))
}

/// Plain text of a stored AMF envelope's parts, unless the message has expired.
fn envelope_text(env: &str) -> Option<String> {
    let v: Value = serde_json::from_str(env).ok()?;
    let expired = v["expiresAt"].as_str().and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok()).is_some_and(|t| t < chrono::Utc::now());
    if expired {
        return None;
    }
    let parts = v["parts"].as_array()?;
    let t: Vec<String> = parts
        .iter()
        .filter_map(|p| p["text"].as_str().map(String::from).or_else(|| p["url"].as_str().map(|u| format!("<{u}>"))).or_else(|| p.get("data").map(|d| d.to_string())))
        .collect();
    (!t.is_empty()).then(|| cap(&t.join("\n"), MAX_TEXT))
}

fn local_row(r: &agentlan_store::rusqlite::Row) -> agentlan_store::rusqlite::Result<ActivityRow> {
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
}

/// This PC's activity log. With `after_seq` 0: the newest `limit` matching rows (paging
/// back as far as needed for an agent filter), oldest first, and a cursor at the newest
/// row. With `after_seq`: the next rows after it in order, at most `limit` scanned; call
/// again from the returned cursor until it stops moving.
pub fn read_local(home: &Path, f: &LogFilter) -> Result<Page> {
    let c = open_ro(&crate::node_dir(home).join("node.db"))?;
    if !has_table(&c, "activity")? {
        bail!("this PC's node hasn't recorded any activity yet: it starts once the node runs AgentLAN 0.2.0-beta.6 or newer");
    }
    let since = f.since.clone().unwrap_or_default();
    let limit = f.limit.max(1);
    let keep = |r: &ActivityRow| f.agent.as_deref().is_none_or(|who| mentions(r, who));
    let mut page = Page::default();
    if f.incremental {
        // Rows are committed in sequence order by the node (one writer), so reading
        // forward from the cursor never skips one.
        let mut st = c.prepare(
            "SELECT seq, ts, kind, agent, msg_id, thread_id, body FROM activity
             WHERE seq > ?1 AND ts >= ?2 AND (?3 IS NULL OR thread_id = ?3) ORDER BY seq ASC LIMIT ?4",
        )?;
        let rows: Vec<ActivityRow> = st.query_map(params![f.after_seq, since, f.thread, limit as i64], local_row)?.collect::<Result<_, _>>()?;
        page.more = rows.len() == limit;
        page.cursor = rows.last().map(|r| r.seq).unwrap_or(f.after_seq);
        page.rows = rows.into_iter().filter(keep).collect();
    } else {
        let top: i64 = c.query_row("SELECT COALESCE(MAX(seq), 0) FROM activity", [], |r| r.get(0))?;
        page.cursor = top;
        let mut before = top + 1;
        let mut st = c.prepare(
            "SELECT seq, ts, kind, agent, msg_id, thread_id, body FROM activity
             WHERE seq < ?1 AND ts >= ?2 AND (?3 IS NULL OR thread_id = ?3) ORDER BY seq DESC LIMIT 2000",
        )?;
        while page.rows.len() < limit {
            let batch: Vec<ActivityRow> = st.query_map(params![before, since, f.thread], local_row)?.collect::<Result<_, _>>()?;
            let n = batch.len();
            if let Some(last) = batch.last() {
                before = last.seq;
            }
            page.rows.extend(batch.into_iter().filter(keep).take(limit - page.rows.len()));
            if n < 2000 {
                break;
            }
        }
        page.rows.reverse();
    }
    if f.with_text {
        for r in page.rows.iter_mut().filter(|r| matches!(r.kind.as_str(), "sent" | "received")) {
            if let Some(m) = &r.msg_id {
                let env: Option<String> = c.query_row("SELECT envelope FROM messages WHERE msg_id=?1", params![m], |x| x.get(0)).optional()?;
                r.text = env.as_deref().and_then(envelope_text);
            }
        }
    }
    Ok(page)
}

/// The server's view of traffic between PCs (on the server PC only): messages the hub
/// relayed, each recipient's latest delivery state (stored, seen, handled, expired; the
/// hub keeps only the latest), and who came online or went offline.
///
/// Without `incremental`: the newest `limit` rows since `since`, oldest first. With it:
/// what came after `net_after`. The cursor follows the hub's own order (insertion order
/// of messages, the hub's delivery-update times, event sequence numbers), never a
/// sender-supplied time. Only the fields shown are read from each message; the text only
/// with `with_text`.
pub fn read_network(home: &Path, f: &LogFilter) -> Result<Page> {
    let path = home.join("hub").join("hub.db");
    if !path.exists() {
        bail!("this PC isn't the server: the network view is on the PC that runs `agentlan up`");
    }
    let c = open_ro(&path)?;
    has_table(&c, "messages")?;
    let since = f.since.clone().unwrap_or_default();
    // An agent filter is applied after reading, so read a wider window for it.
    let lim = if f.agent.is_some() && !f.incremental { (f.limit.max(1) * 20).min(20_000) } else { f.limit.max(1) } as i64;
    let names: std::collections::HashMap<String, String> = {
        let mut st = c.prepare("SELECT a.agent_id, a.name || '@' || m.alias FROM agents a JOIN machines m USING(machine_id)")?;
        st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?.collect::<Result<_, _>>()?
    };
    let name = |id: &str| names.get(id).cloned().unwrap_or_else(|| id.to_string());
    // Cursor: (last message rowid, last delivery update time, last event seq).
    let (mut m_cur, mut d_cur, mut e_cur): (i64, String, i64) = match (&f.net_after, f.incremental) {
        (Some(cur), true) => {
            let mut it = cur.splitn(3, '|');
            (it.next().and_then(|x| x.parse().ok()).unwrap_or(0), it.next().unwrap_or("").to_string(), it.next().and_then(|x| x.parse().ok()).unwrap_or(0))
        }
        _ => (
            c.query_row("SELECT COALESCE(MAX(rowid), 0) FROM messages", [], |r| r.get(0))?,
            c.query_row("SELECT COALESCE(MAX(updated), '') FROM deliveries", [], |r| r.get(0))?,
            c.query_row("SELECT COALESCE(MAX(seq), 0) FROM events", [], |r| r.get(0))?,
        ),
    };
    let (m_top, d_top, e_top) = (m_cur, d_cur.clone(), e_cur);
    let inc = f.incremental;
    let mut rows = vec![];
    let mut full = false;
    let msg_cols = "json_extract(m.envelope,'$.from.handle'), json_extract(m.envelope,'$.from.agent'), json_extract(m.envelope,'$.intent'),
         substr(json_extract(m.envelope,'$.subject'), 1, 301), json_extract(m.envelope,'$.threadId'), json(json_extract(m.envelope,'$.to')),
         json_extract(m.envelope,'$.inReplyTo')";
    let thread_sql = "(?3 IS NULL OR json_extract(m.envelope,'$.threadId') = ?3)";
    type MsgCols = (Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>);
    let msg_cols_of = |r: &agentlan_store::rusqlite::Row, i: usize| -> agentlan_store::rusqlite::Result<MsgCols> {
        Ok((r.get(i)?, r.get(i + 1)?, r.get(i + 2)?, r.get(i + 3)?, r.get(i + 4)?, r.get(i + 5)?, r.get(i + 6)?))
    };
    {
        let sql = if inc {
            format!("SELECT m.rowid, m.msg_id, m.created_at, {msg_cols}, CASE WHEN ?4 THEN m.envelope END FROM messages m WHERE m.rowid > ?1 AND m.created_at >= ?2 AND {thread_sql} ORDER BY m.rowid ASC LIMIT ?5")
        } else {
            format!("SELECT m.rowid, m.msg_id, m.created_at, {msg_cols}, CASE WHEN ?4 THEN m.envelope END FROM messages m WHERE m.rowid <= ?1 AND m.created_at >= ?2 AND {thread_sql} ORDER BY m.rowid DESC LIMIT ?5")
        };
        let mut st = c.prepare(&sql)?;
        let found: Vec<(i64, String, String, MsgCols, Option<String>)> = st
            .query_map(params![m_cur, since, f.thread, f.with_text, lim], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, msg_cols_of(r, 3)?, r.get(10)?)))?
            .collect::<Result<_, _>>()?;
        full |= found.len() as i64 == lim;
        let mut rc = c.prepare("SELECT recipient FROM deliveries WHERE msg_id=?1")?;
        for (rowid, id, at, (from, from_agent, intent, subject, thread, to, in_reply), env) in found {
            if inc {
                m_cur = m_cur.max(rowid);
            }
            let recipients: Vec<String> = rc.query_map(params![id], |r| r.get::<_, String>(0))?.filter_map(|x| x.ok()).map(|x| name(&x)).collect();
            let to: Value = to.as_deref().and_then(|t| serde_json::from_str(t).ok()).unwrap_or(Value::Null);
            rows.push(ActivityRow {
                seq: 0,
                ts: at,
                kind: "message".into(),
                agent: from_agent,
                msg_id: Some(id),
                thread_id: thread,
                body: json!({"from": from, "to": to, "intent": intent, "subject": subject, "inReplyTo": in_reply, "recipients": recipients}),
                text: env.as_deref().and_then(envelope_text),
            });
        }
    }
    {
        let sql = if inc {
            format!("SELECT d.msg_id, d.recipient, d.state, d.updated, {msg_cols} FROM deliveries d LEFT JOIN messages m USING(msg_id) WHERE d.updated >= ?1 AND d.updated >= ?2 AND {thread_sql} AND ?4 = ?4 ORDER BY d.updated ASC LIMIT ?5")
        } else {
            format!("SELECT d.msg_id, d.recipient, d.state, d.updated, {msg_cols} FROM deliveries d LEFT JOIN messages m USING(msg_id) WHERE d.updated <= ?1 AND d.updated >= ?2 AND {thread_sql} AND ?4 = ?4 ORDER BY d.updated DESC LIMIT ?5")
        };
        let mut st = c.prepare(&sql)?;
        let found: Vec<(String, String, String, String, MsgCols)> = st
            .query_map(params![d_cur, since, f.thread, true, lim], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, msg_cols_of(r, 4)?)))?
            .collect::<Result<_, _>>()?;
        full |= found.len() as i64 == lim;
        for (id, rcpt, state, updated, (from, _, intent, subject, thread, _, _)) in found {
            if inc && updated > d_cur {
                d_cur = updated.clone();
            }
            rows.push(ActivityRow {
                seq: 0,
                ts: updated,
                kind: "delivery".into(),
                agent: Some(rcpt.clone()),
                msg_id: Some(id),
                thread_id: thread,
                body: json!({"handle": name(&rcpt), "state": state, "from": from, "subject": subject, "intent": intent}),
                text: None,
            });
        }
    }
    if f.thread.is_none() {
        let sql = if inc {
            "SELECT seq, ts, body FROM events WHERE seq > ?1 AND ts >= ?2 ORDER BY seq ASC LIMIT ?3"
        } else {
            "SELECT seq, ts, body FROM events WHERE seq <= ?1 AND ts >= ?2 ORDER BY seq DESC LIMIT ?3"
        };
        let mut st = c.prepare(sql)?;
        let evs: Vec<(i64, String, String)> = st.query_map(params![e_cur, since, lim], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?.collect::<Result<_, _>>()?;
        full |= evs.len() as i64 == lim;
        for (seq, ts, body) in evs {
            if inc {
                e_cur = e_cur.max(seq);
            }
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
    // Time order for display; a message before its own delivery states.
    rows.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| (a.kind != "message").cmp(&(b.kind != "message"))));
    if let Some(who) = &f.agent {
        rows.retain(|r| mentions(r, who));
    }
    if !inc {
        let skip = rows.len().saturating_sub(f.limit.max(1));
        rows = rows.split_off(skip);
        (m_cur, d_cur, e_cur) = (m_top, d_top, e_top);
    }
    for (i, r) in rows.iter_mut().enumerate() {
        r.seq = i as i64 + 1;
    }
    Ok(Page { rows, cursor: 0, net_cursor: format!("{m_cur}|{d_cur}|{e_cur}"), more: inc && full })
}
