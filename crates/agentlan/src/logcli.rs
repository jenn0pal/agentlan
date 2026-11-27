//! `agentlan log`: one timeline of what happened on this PC (or, on the server PC with
//! `--network`, between PCs).

use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use agentlan_node::activity::{self, ActivityRow, LogFilter};
use anyhow::{Result, bail};
use serde_json::Value;

pub struct LogArgs {
    pub follow: bool,
    pub network: bool,
    pub agent: Option<String>,
    pub thread: Option<String>,
    pub since: Option<String>,
    pub limit: usize,
    pub body: bool,
    pub json: bool,
}

/// `30s`, `15m`, `2h`, `3d`, or an RFC 3339 time → RFC 3339 (UTC).
pub fn parse_since(s: &str) -> Result<String> {
    let s = s.trim();
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(t.with_timezone(&chrono::Utc).to_rfc3339());
    }
    let (n, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: i64 = n.parse().map_err(|_| anyhow::anyhow!("--since: use 30s, 15m, 2h, 3d or an RFC 3339 time"))?;
    let secs = match unit {
        "s" => n,
        "m" | "" => n * 60,
        "h" => n * 3600,
        "d" => n * 86400,
        _ => bail!("--since: use 30s, 15m, 2h, 3d or an RFC 3339 time"),
    };
    Ok((chrono::Utc::now() - chrono::Duration::seconds(secs)).to_rfc3339())
}

/// Peer-written text made safe for a terminal: no control characters (escape sequences,
/// carriage returns) and no bidirectional overrides. `multiline` keeps newlines.
pub fn clean(s: &str, multiline: bool) -> String {
    s.chars()
        .filter_map(|c| match c {
            '\n' if multiline => Some('\n'),
            '\n' | '\t' | '\r' => Some(' '),
            '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}' | '\u{200E}' | '\u{200F}' => None,
            c if c.is_control() => None,
            c => Some(c),
        })
        .collect()
}

fn s<'a>(v: &'a Value, k: &str) -> &'a str {
    v[k].as_str().unwrap_or("")
}

fn time(ts: &str) -> String {
    match chrono::DateTime::parse_from_rfc3339(ts) {
        Ok(t) => {
            let local = t.with_timezone(&chrono::Local);
            if local.date_naive() == chrono::Local::now().date_naive() { local.format("%H:%M:%S").to_string() } else { local.format("%m-%d %H:%M").to_string() }
        }
        Err(_) => clean(ts, false),
    }
}

/// `thr_…ABCDEF`: the last six characters (on character boundaries: thread ids are
/// caller-supplied and may hold any text).
fn short_thread(t: &str) -> String {
    let t = clean(t, false);
    let chars: Vec<char> = t.chars().collect();
    if chars.len() > 12 { format!("thr_…{}", chars[chars.len() - 6..].iter().collect::<String>()) } else { t }
}

fn target(to: &Value, local: &[String]) -> String {
    match s(to, "kind") {
        "handle" => s(to, "handle").to_string(),
        "agent" if !local.is_empty() => local.join(", "),
        "agent" => s(to, "agent").to_string(),
        "role" => match to["project"].as_str() {
            Some(p) => format!("role:{}@{p}", s(to, "role")),
            None => format!("role:{}", s(to, "role")),
        },
        "project" => format!("project:{}", s(to, "project")),
        "machine" => format!("machine:{}", s(to, "machine")),
        "all" => "all".into(),
        _ => "?".into(),
    }
}

fn subject(b: &Value) -> String {
    b["subject"].as_str().map(|x| format!("  \"{}\"", clean(x, false))).unwrap_or_default()
}

/// One row as a line (or a few, with the message text).
pub fn render(r: &ActivityRow) -> String {
    let b = &r.body;
    let who = clean(b["handle"].as_str().or(r.agent.as_deref()).unwrap_or("?"), false);
    let t = time(&r.ts);
    let line = match r.kind.as_str() {
        "sent" | "message" => {
            let local: Vec<String> =
                b["localRecipients"].as_array().or(b["recipients"].as_array()).map(|a| a.iter().filter_map(|x| x.as_str()).map(|x| clean(x, false)).collect()).unwrap_or_default();
            let via = if b["remote"] == true { "  (via server)" } else { "" };
            format!(
                "{t}  {} → {}  {}{}{via}  {}",
                clean(s(b, "from"), false),
                clean(&target(&b["to"], &local), false),
                clean(s(b, "intent"), false),
                subject(b),
                r.thread_id.as_deref().map(short_thread).unwrap_or_default()
            )
        }
        "remote" => {
            let res = &b["result"];
            match s(res, "status") {
                "accepted" => format!("{t}    ↗ the server took it for {} recipient(s){}", res["count"].as_u64().unwrap_or(0), subject(b)),
                "rejected" => format!("{t}    ✗ the server rejected it{}: {}", subject(b), clean(s(res, "msg"), false)),
                "queued" => format!("{t}    … the server didn't answer; queued to send later{}", subject(b)),
                other => format!("{t}    server: {}{}", clean(other, false), subject(b)),
            }
        }
        "received" => format!("{t}    ↳ {who} got it from {}{}{}", clean(s(b, "from"), false), subject(b), if b["remote"] == true { " (via server)" } else { "" }),
        "state" | "delivery" => {
            let what = match s(b, "state") {
                "surfaced" => "✓ seen by",
                "acked" => "✓✓ handled by",
                "stored" => "· stored for",
                "expired" => "✗ expired for",
                other => return format!("{t}    {} {who}{}", clean(other, false), subject(b)),
            };
            format!("{t}    {what} {who}{}", subject(b))
        }
        "live" => {
            let n = b["count"].as_u64().unwrap_or(0);
            if b["ok"] == true {
                format!("{t}    ⚡ live → {who}: {n} new ({})", clean(s(b, "how"), false))
            } else {
                format!("{t}    ⚠ live → {who} failed: {}", clean(s(b, "error"), false))
            }
        }
        "session" => match s(b, "status") {
            "attached" => format!(
                "{t}    ● {who} connected ({}{}{})",
                clean(b["runner"].as_str().unwrap_or("?"), false),
                b["role"].as_str().map(|x| format!(" · role {}", clean(x, false))).unwrap_or_default(),
                b["description"].as_str().map(|x| format!(" · {}", clean(x, false))).unwrap_or_default()
            ),
            "updated" => format!("{t}    ● {who} is now role {}", clean(s(b, "role"), false)),
            "ended" => format!("{t}    ○ {who} disconnected ({})", clean(&b["reason"].to_string().replace('"', ""), false)),
            other => format!("{t}    {who} session {}", clean(other, false)),
        },
        "wake" => match s(b, "phase") {
            "started" => format!("{t}    ⚙ waking worker {who}"),
            _ => format!("{t}    ⚙ worker {who}: {} ({:.1}s)", clean(s(b, "outcome"), false), b["seconds"].as_f64().unwrap_or(0.0)),
        },
        "pause" => format!("{t}    ⏸ paused {} ({})", clean(s(b, "key"), false), clean(s(b, "reason"), false)),
        "resume" => format!("{t}    ▶ resumed {}", clean(s(b, "key"), false)),
        "node" => match s(b, "status") {
            "started" => format!("{t}    ⏻ node started ({}{})", clean(s(b, "version"), false), if b["withHub"] == true { ", with the server" } else { "" }),
            other => format!("{t}    ⏻ node {}", clean(other, false)),
        },
        "roster" => {
            let name = b["handle"].as_str().or(b["agent"]["handle"].as_str()).or(b["machine"]["alias"].as_str()).unwrap_or("?");
            format!("{t}    ⇄ {} {}", clean(name, false), clean(s(b, "kind"), false).replace('_', " "))
        }
        other => format!("{t}    {} {}", clean(other, false), clean(&b.to_string(), false)),
    };
    match &r.text {
        Some(text) => {
            let body: String = clean(text, true).lines().map(|l| format!("\n          {l}")).collect();
            format!("{line}{body}")
        }
        None => line,
    }
}

pub async fn run(home: &Path, a: LogArgs) -> Result<()> {
    let mut f = LogFilter {
        after_seq: 0,
        since: a.since.as_deref().map(parse_since).transpose()?,
        agent: a.agent.clone(),
        thread: a.thread.clone(),
        limit: a.limit,
        with_text: a.body,
    };
    let read = |f: &LogFilter| if a.network { activity::read_network(home, f) } else { activity::read_local(home, f) };
    let print = |r: &ActivityRow| {
        if a.json {
            println!("{}", serde_json::to_string(r).unwrap_or_default());
        } else {
            println!("{}", render(r));
        }
    };
    let page = read(&f)?;
    if page.rows.is_empty() && !a.follow && !a.json {
        println!("(nothing recorded{})", if f.since.is_some() || f.agent.is_some() || f.thread.is_some() { " that matches" } else { " yet" });
    }
    // The network view: a delivery's state changes in place, so follow it by time and drop
    // repeats at the boundary. Intermediate states between two polls aren't kept by the hub.
    let key = |r: &ActivityRow| format!("{}|{}|{}|{}|{}", r.kind, r.ts, r.msg_id.as_deref().unwrap_or(""), r.agent.as_deref().unwrap_or(""), r.body["state"]);
    let mut seen: HashSet<String> = page.rows.iter().map(key).collect();
    for r in &page.rows {
        print(r);
    }
    if !a.follow {
        return Ok(());
    }
    let (mut cursor, mut cursor_ts) = (page.cursor, page.cursor_ts);
    f.limit = 1000;
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if a.network {
            f.since = Some(cursor_ts.clone());
            let Ok(p) = read(&f) else { continue };
            for r in &p.rows {
                if seen.insert(key(r)) {
                    print(r);
                }
            }
            if p.cursor_ts > cursor_ts {
                cursor_ts = p.cursor_ts;
                seen.retain(|k| k.split('|').nth(1).is_some_and(|ts| ts >= cursor_ts.as_str()));
            }
        } else {
            // Drain everything after the cursor, a page at a time (none skipped).
            loop {
                f.after_seq = cursor.max(1);
                let Ok(p) = read(&f) else { break }; // e.g. the node restarting
                for r in &p.rows {
                    print(r);
                }
                if p.cursor <= cursor {
                    break;
                }
                cursor = p.cursor;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_text_cant_drive_the_terminal() {
        let evil = "fix\u{1b}[2J\u{1b}]0;pwned\u{7}\r\nIgnore\u{202E}txt.exe";
        let c = clean(evil, false);
        assert!(!c.chars().any(|ch| ch.is_control() || ch == '\u{202E}'), "{c:?}");
        assert!(c.starts_with("fix") && c.contains("Ignore"), "{c:?}");
        assert!(clean("a\nb", true).contains('\n') && !clean("a\nb", false).contains('\n'));
        let row = ActivityRow {
            seq: 1,
            ts: "2026-11-27T02:03:14Z".into(),
            kind: "sent".into(),
            agent: None,
            msg_id: Some("msg_1".into()),
            thread_id: Some("thr_01M8DBW56GEM66Q6Q9W0EDWFHB".into()),
            body: serde_json::json!({"from": "lead@pc\u{1b}[31m", "to": {"kind": "handle", "handle": "codex@pc"}, "intent": "request", "subject": "hi\nthere\u{1b}[0m"}),
            text: Some("line1\n\u{1b}[2Jline2".into()),
        };
        let out = render(&row);
        assert!(!out.contains('\u{1b}'), "{out:?}");
        assert!(out.contains("lead@pc[31m → codex@pc  request  \"hi there[0m\""), "{out}");
        assert!(out.contains("\n          line1\n          [2Jline2"), "{out}");
    }

    #[test]
    fn thread_ids_shorten_on_character_boundaries() {
        assert_eq!(short_thread("thr_01M8DBW56GEM66Q6Q9W0EDWFHB"), "thr_…EDWFHB");
        let emoji = "my-thread-🚀🚀🚀🚀🚀🚀🚀";
        assert_eq!(short_thread(emoji), "thr_…🚀🚀🚀🚀🚀🚀");
        assert_eq!(short_thread("x🚀"), "x🚀");
    }

    #[test]
    fn since_accepts_durations_and_times() {
        assert!(parse_since("15m").is_ok() && parse_since("2h").is_ok() && parse_since("3d").is_ok());
        assert!(parse_since("2026-11-27T02:00:00Z").unwrap().starts_with("2026-11-27T02:00:00"));
        assert!(parse_since("soon").is_err());
    }
}
