//! The hub's single-owner state: registry, presence, routing and mailboxes.
//! Runs on one dedicated thread; every database write happens here.
//!
//! Each command runs inside one SQLite transaction. Outgoing frames are buffered
//! and only published after the commit, so peers never see uncommitted state.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use agentlan_proto::amf::{Amf, Origin, Pick, Target};
use agentlan_proto::records::{AgentProfile, AgentRecord, MachineRecord, MachineReported, PresenceState, Readiness};
use agentlan_proto::wire::{DeliveryState, EventKind, HubToNode, NodeToHub, RegistryEvent, SessionOutcome};
use agentlan_proto::{Ts, now};
use agentlan_store::rusqlite::{Connection, OptionalExtension, params};
use agentlan_store::{meta_set, meta_u64};
use anyhow::Result;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

pub const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS invites(token_hash TEXT PRIMARY KEY, expires TEXT NOT NULL, used_by TEXT);
CREATE TABLE IF NOT EXISTS machines(
  machine_id TEXT PRIMARY KEY, pubkey TEXT NOT NULL UNIQUE, alias TEXT NOT NULL UNIQUE,
  reported TEXT, labels TEXT NOT NULL DEFAULT '[]', state TEXT NOT NULL DEFAULT 'offline',
  enrolled_at TEXT NOT NULL, last_seen TEXT, revoked INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS agents(
  agent_id TEXT PRIMARY KEY, machine_id TEXT NOT NULL REFERENCES machines(machine_id), name TEXT NOT NULL,
  profile TEXT NOT NULL, state TEXT NOT NULL, readiness TEXT NOT NULL, session_id TEXT,
  fence INTEGER NOT NULL DEFAULT 0, since TEXT, end_reason TEXT, retired INTEGER NOT NULL DEFAULT 0,
  UNIQUE(machine_id, name));
CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY, ts TEXT NOT NULL, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS messages(
  msg_id TEXT PRIMARY KEY, from_agent TEXT NOT NULL, origin_machine TEXT NOT NULL, envelope TEXT NOT NULL,
  remote_count INTEGER NOT NULL, created_at TEXT NOT NULL, expires_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS deliveries(
  msg_id TEXT NOT NULL, recipient TEXT NOT NULL, machine_id TEXT NOT NULL, pair_seq INTEGER NOT NULL,
  state TEXT NOT NULL, updated TEXT NOT NULL, PRIMARY KEY(msg_id, recipient));
CREATE TABLE IF NOT EXISTS pair_seq(sender TEXT NOT NULL, recipient TEXT NOT NULL, next INTEGER NOT NULL, PRIMARY KEY(sender, recipient));
CREATE TABLE IF NOT EXISTS journal(msg_id TEXT PRIMARY KEY, machine_id TEXT NOT NULL, body TEXT NOT NULL);
"#;

const FANOUT_CAP: usize = 10;
/// Max deliveries in flight (sent, not yet receipted) per connection.
const WINDOW: usize = 256;
const SNAPSHOT_PAGE: usize = 200;

pub fn token_hash(token: &str) -> String {
    data_encoding::HEXLOWER.encode(&Sha256::digest(token.as_bytes()))
}

fn s<T: Serialize>(v: &T) -> String {
    match serde_json::to_value(v).expect("serializable") {
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    }
}

fn p<T: DeserializeOwned>(text: &str) -> T {
    serde_json::from_value(serde_json::Value::String(text.to_string()))
        .or_else(|_| serde_json::from_str(text))
        .expect("valid stored enum")
}

fn ts(t: &str) -> Option<Ts> {
    t.parse().ok()
}

fn state_rank(s: DeliveryState) -> u8 {
    match s {
        DeliveryState::Accepted => 0,
        DeliveryState::Stored => 1,
        DeliveryState::Surfaced => 2,
        DeliveryState::Acked => 3,
        DeliveryState::Expired => 4,
    }
}

pub struct MachineRow {
    pub machine_id: String,
    pub alias: String,
    pub revoked: bool,
}

pub enum Cmd {
    Lookup { pubkey: String, reply: oneshot::Sender<Option<MachineRow>> },
    Enroll { pubkey: String, token: String, alias: String, reply: oneshot::Sender<Result<(String, String), String>> },
    Connected { machine_id: String, cgen: u64, tx: mpsc::Sender<HubToNode>, kill: oneshot::Sender<()> },
    Frame { machine_id: String, cgen: u64, frame: NodeToHub },
    Disconnected { machine_id: String, cgen: u64 },
    Tick,
}

struct Conn {
    cgen: u64,
    tx: mpsc::Sender<HubToNode>,
    kill: Option<oneshot::Sender<()>>,
    /// Deliveries only flow after this connection's resync completed (keeps FIFO).
    ready: bool,
    inflight: HashSet<(String, String)>,
}

/// Terminal rejection (reported to the sender) vs. storage failure (retry later).
enum SubmitErr {
    Reject(&'static str, String),
    Db(anyhow::Error),
}

impl<E: Into<anyhow::Error>> std::convert::From<E> for SubmitErr {
    fn from(e: E) -> Self {
        SubmitErr::Db(e.into())
    }
}

pub struct Hub {
    db: Connection,
    lease: Duration,
    conns: HashMap<String, Conn>,
    deadlines: HashMap<String, Instant>,
    last_frame: HashMap<String, Instant>,
    /// Frames to publish after the current transaction commits.
    outq: Vec<(String, HubToNode)>,
    /// Machines whose delivery pump should run after commit.
    pump_after: HashSet<String>,
}

impl Hub {
    pub fn new(db: Connection, lease: Duration) -> Result<Hub> {
        agentlan_store::migrate(&db, SCHEMA)?;
        // Recover the event sequence if a crash left meta behind the event log.
        let max_ev: i64 = db.query_row("SELECT COALESCE(MAX(seq),0) FROM events", [], |r| r.get(0))?;
        if (max_ev as u64) > meta_u64(&db, "seq")? {
            meta_set(&db, "seq", &max_ev.to_string())?;
        }
        // Hub restart: nobody is connected yet. Sessions stay unconfirmed until nodes resync.
        db.execute("UPDATE agents SET state='lost', end_reason='hub_restart', readiness='detached' WHERE state='online'", [])?;
        db.execute("UPDATE machines SET state='lost' WHERE state='online'", [])?;
        Ok(Hub {
            db,
            lease,
            conns: HashMap::new(),
            deadlines: HashMap::new(),
            last_frame: HashMap::new(),
            outq: Vec::new(),
            pump_after: HashSet::new(),
        })
    }

    pub fn run(mut self, mut rx: mpsc::UnboundedReceiver<Cmd>) {
        while let Some(cmd) = rx.blocking_recv() {
            self.handle(cmd);
        }
    }

    fn handle(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Connected { machine_id, cgen, tx, kill } => {
                if let Some(mut old) = self.conns.remove(&machine_id) {
                    info!("hub: {machine_id} reconnected (gen {} → {cgen}); closing old link", old.cgen);
                    if let Some(k) = old.kill.take() {
                        let _ = k.send(());
                    }
                }
                self.conns.insert(machine_id, Conn { cgen, tx, kill: Some(kill), ready: false, inflight: HashSet::new() });
            }
            Cmd::Disconnected { machine_id, cgen } => {
                if self.conns.get(&machine_id).is_some_and(|c| c.cgen == cgen) {
                    self.conns.remove(&machine_id);
                    self.start_lease(&machine_id);
                    debug!("hub: {machine_id} disconnected; lease running");
                }
            }
            Cmd::Lookup { pubkey, reply } => {
                let row = self
                    .db
                    .query_row("SELECT machine_id, alias, revoked FROM machines WHERE pubkey=?1", params![pubkey], |r| {
                        Ok(MachineRow { machine_id: r.get(0)?, alias: r.get(1)?, revoked: r.get::<_, i64>(2)? != 0 })
                    })
                    .optional()
                    .unwrap_or(None);
                let _ = reply.send(row);
            }
            Cmd::Enroll { pubkey, token, alias, reply } => {
                let r = self.in_txn(|h| h.enroll(&pubkey, &token, &alias));
                let _ = reply.send(r.map_err(|e| e.to_string()));
            }
            Cmd::Tick => {
                let now_i = Instant::now();
                let expired: Vec<String> = self.deadlines.iter().filter(|(_, d)| **d <= now_i).map(|(k, _)| k.clone()).collect();
                if !expired.is_empty() && self.in_txn(|h| h.expire_leases(&expired)).is_ok() {
                    // Only forget the deadlines once the expiry is durable.
                    for mid in expired {
                        self.deadlines.remove(&mid);
                        if let Some(mut c) = self.conns.remove(&mid) {
                            if let Some(k) = c.kill.take() {
                                let _ = k.send(());
                            }
                        }
                    }
                }
            }
            Cmd::Frame { machine_id, cgen, frame } => {
                if !self.conns.get(&machine_id).is_some_and(|c| c.cgen == cgen) {
                    return; // frame from a superseded connection
                }
                self.last_frame.insert(machine_id.clone(), Instant::now());
                let _ = self.in_txn(|h| {
                    h.db.execute("UPDATE machines SET last_seen=?1 WHERE machine_id=?2", params![now().to_rfc3339(), machine_id])?;
                    h.on_frame(&machine_id, frame)
                });
            }
        }
    }

    /// Run `f` in one transaction; publish buffered frames only after commit.
    fn in_txn<T>(&mut self, f: impl FnOnce(&mut Hub) -> Result<T>) -> Result<T> {
        self.db.execute_batch("BEGIN IMMEDIATE")?;
        match f(self) {
            Ok(v) => match self.db.execute_batch("COMMIT") {
                Ok(()) => {
                    self.flush();
                    Ok(v)
                }
                Err(e) => {
                    let _ = self.db.execute_batch("ROLLBACK");
                    self.discard();
                    warn!("hub: commit failed: {e}");
                    Err(e.into())
                }
            },
            Err(e) => {
                let _ = self.db.execute_batch("ROLLBACK");
                self.discard();
                warn!("hub: command failed (rolled back): {e:#}");
                Err(e)
            }
        }
    }

    fn discard(&mut self) {
        self.outq.clear();
        self.pump_after.clear();
    }

    fn flush(&mut self) {
        let q = std::mem::take(&mut self.outq);
        for (mid, f) in q {
            self.deliver_now(&mid, f);
        }
        let pumps: Vec<String> = self.pump_after.drain().collect();
        for mid in pumps {
            if let Err(e) = self.pump(&mid) {
                warn!("hub: pump for {mid} failed: {e:#}");
            }
        }
    }

    fn deliver_now(&mut self, mid: &str, f: HubToNode) {
        let failed = match self.conns.get(mid) {
            Some(c) => c.tx.try_send(f).is_err(),
            None => false,
        };
        if failed {
            warn!("hub: outbound queue to {mid} full or closed; dropping link");
            if let Some(mut c) = self.conns.remove(mid) {
                if let Some(k) = c.kill.take() {
                    let _ = k.send(());
                }
            }
            self.start_lease(mid);
        }
    }

    /// One lease deadline measured from the last frame actually received.
    fn start_lease(&mut self, mid: &str) {
        let base = self.last_frame.get(mid).copied().unwrap_or_else(Instant::now);
        self.deadlines.entry(mid.to_string()).or_insert(base + self.lease);
    }

    fn send(&mut self, mid: &str, f: HubToNode) {
        self.outq.push((mid.to_string(), f));
    }

    // ------------------------------------------------------------------ delivery pump

    /// Send accepted deliveries for `mid` in commit order, bounded by WINDOW.
    fn pump(&mut self, mid: &str) -> Result<()> {
        let budget = match self.conns.get(mid) {
            Some(c) if c.ready => WINDOW.saturating_sub(c.inflight.len()),
            _ => return Ok(()),
        };
        if budget == 0 {
            return Ok(());
        }
        let mut st = self.db.prepare(
            "SELECT d.msg_id, d.recipient, d.pair_seq, m.envelope FROM deliveries d JOIN messages m USING(msg_id)
             WHERE d.machine_id=?1 AND d.state='accepted' AND m.expires_at > ?2 ORDER BY d.rowid LIMIT ?3",
        )?;
        let limit = (WINDOW * 2) as i64;
        let rows: Vec<(String, String, i64, String)> =
            st.query_map(params![mid, now().to_rfc3339(), limit], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?.collect::<Result<_, _>>()?;
        drop(st);
        let mut sent = 0;
        for (msg_id, recipient, seq, env) in rows {
            if sent >= budget {
                break;
            }
            let key = (msg_id, recipient.clone());
            if self.conns.get(mid).is_some_and(|c| c.inflight.contains(&key)) {
                continue;
            }
            let envelope: Amf = serde_json::from_str(&env)?;
            match self.conns.get_mut(mid) {
                Some(c) => {
                    c.inflight.insert(key);
                }
                None => break, // link dropped: nothing later may overtake this delivery
            }
            self.deliver_now(mid, HubToNode::Deliver { recipient, pair_seq: seq as u64, envelope });
            if !self.conns.contains_key(mid) {
                break;
            }
            sent += 1;
        }
        Ok(())
    }

    // ------------------------------------------------------------------ enrolment

    fn enroll(&mut self, pubkey: &str, token: &str, alias: &str) -> Result<(String, String)> {
        let pk = agentlan_net::keys::from_b32(pubkey)?;
        let mid = agentlan_net::keys::machine_id(&pk);
        let h = token_hash(token);
        let inv: Option<(String, Option<String>)> = self
            .db
            .query_row("SELECT expires, used_by FROM invites WHERE token_hash=?1", params![h], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?;
        let Some((expires, used_by)) = inv else { anyhow::bail!("unknown join token") };
        match &used_by {
            Some(u) if u != &mid => anyhow::bail!("join token already used"),
            Some(_) => {}
            None if ts(&expires).is_none_or(|e| e < now()) => anyhow::bail!("join token expired"),
            None => {}
        }
        let existing: Option<String> = self.db.query_row("SELECT alias FROM machines WHERE machine_id=?1", params![mid], |r| r.get(0)).optional()?;
        let alias = match existing {
            Some(a) => a,
            None => {
                let base = agentlan_proto::ids::normalize_name(alias);
                let base = if base.is_empty() { "pc".to_string() } else { base };
                let mut cand = base.clone();
                let mut i = 2;
                while self.db.query_row("SELECT 1 FROM machines WHERE alias=?1", params![cand], |_| Ok(())).optional()?.is_some() {
                    cand = format!("{base}-{i}");
                    i += 1;
                }
                self.db.execute(
                    "INSERT INTO machines(machine_id, pubkey, alias, enrolled_at) VALUES(?1,?2,?3,?4)",
                    params![mid, pubkey, cand, now().to_rfc3339()],
                )?;
                cand
            }
        };
        self.db.execute("UPDATE invites SET used_by=?1 WHERE token_hash=?2", params![mid, h])?;
        info!("hub: enrolled {mid} as '{alias}'");
        Ok((mid, alias))
    }

    // ------------------------------------------------------------------ records

    fn agent_records(&self, where_sql: &str, arg: Option<&str>) -> Result<Vec<AgentRecord>> {
        let sql = format!(
            "SELECT a.profile, a.machine_id, m.alias, a.state, a.readiness, a.session_id, a.since, a.end_reason
             FROM agents a JOIN machines m USING(machine_id) {where_sql} ORDER BY a.rowid"
        );
        let mut st = self.db.prepare(&sql)?;
        let map = |r: &agentlan_store::rusqlite::Row| -> agentlan_store::rusqlite::Result<AgentRecord> {
            let profile: AgentProfile = serde_json::from_str(&r.get::<_, String>(0)?).expect("profile json");
            let alias: String = r.get(2)?;
            Ok(AgentRecord {
                handle: agentlan_proto::ids::handle(&profile.name, &alias),
                profile,
                machine_id: r.get(1)?,
                state: p(&r.get::<_, String>(3)?),
                readiness: p(&r.get::<_, String>(4)?),
                session_id: r.get(5)?,
                since: r.get::<_, Option<String>>(6)?.as_deref().and_then(ts),
                end_reason: r.get::<_, Option<String>>(7)?.map(|e| p(&e)),
            })
        };
        let rows = match arg {
            Some(a) => st.query_map(params![a], map)?.collect::<Result<Vec<_>, _>>()?,
            None => st.query_map([], map)?.collect::<Result<Vec<_>, _>>()?,
        };
        Ok(rows)
    }

    fn agent(&self, agent_id: &str) -> Result<Option<AgentRecord>> {
        Ok(self.agent_records("WHERE a.agent_id=?1", Some(agent_id))?.into_iter().next())
    }

    fn machine_records(&self, one: Option<&str>) -> Result<Vec<MachineRecord>> {
        let sql = "SELECT m.machine_id, m.alias, m.reported, m.labels, m.pubkey, m.state, m.enrolled_at, m.last_seen,
                    (SELECT COUNT(*) FROM agents a WHERE a.machine_id=m.machine_id AND a.state='online')
                   FROM machines m WHERE (?1 IS NULL OR m.machine_id=?1) AND m.revoked=0 ORDER BY m.rowid";
        let mut st = self.db.prepare(sql)?;
        let rows = st
            .query_map(params![one], |r| {
                let pk: String = r.get(4)?;
                Ok(MachineRecord {
                    machine_id: r.get(0)?,
                    alias: r.get(1)?,
                    reported: r.get::<_, Option<String>>(2)?.and_then(|j| serde_json::from_str::<MachineReported>(&j).ok()),
                    labels: serde_json::from_str(&r.get::<_, String>(3)?).unwrap_or_default(),
                    key_fp: agentlan_net::keys::from_b32(&pk).map(|k| agentlan_net::keys::fingerprint(&k)).unwrap_or_default(),
                    state: p(&r.get::<_, String>(5)?),
                    enrolled_at: ts(&r.get::<_, String>(6)?).unwrap_or_else(now),
                    last_seen: r.get::<_, Option<String>>(7)?.as_deref().and_then(ts),
                    agents_online: r.get::<_, i64>(8)? as u32,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // ------------------------------------------------------------------ events

    /// Append a registry event (inside the current transaction) and queue it for all peers.
    fn emit(&mut self, kind: EventKind, agent: Option<AgentRecord>, machine: Option<MachineRecord>) -> Result<()> {
        let seq = meta_u64(&self.db, "seq")? + 1;
        let ev = RegistryEvent { seq, kind, agent, machine };
        self.db.execute("INSERT INTO events(seq, ts, body) VALUES(?1,?2,?3)", params![seq as i64, now().to_rfc3339(), serde_json::to_string(&ev)?])?;
        meta_set(&self.db, "seq", &seq.to_string())?;
        let ids: Vec<String> = self.conns.keys().cloned().collect();
        for id in ids {
            self.send(&id, HubToNode::Event { event: ev.clone() });
        }
        Ok(())
    }

    fn emit_agent(&mut self, kind: EventKind, agent_id: &str) -> Result<()> {
        let rec = self.agent(agent_id)?;
        self.emit(kind, rec, None)
    }

    fn expire_leases(&mut self, expired: &[String]) -> Result<()> {
        for mid in expired {
            let mid = mid.clone();
            info!("hub: lease expired for {mid}");
            let online = self.agent_records("WHERE a.machine_id=?1 AND a.state='online'", Some(&mid))?;
            self.db.execute(
                "UPDATE agents SET state='lost', readiness='detached', end_reason='lease_expired' WHERE machine_id=?1 AND state='online'",
                params![mid],
            )?;
            self.db.execute("UPDATE machines SET state='lost' WHERE machine_id=?1", params![mid])?;
            for a in online {
                self.emit_agent(EventKind::Lost, &a.profile.agent_id)?;
            }
            let m = self.machine_records(Some(&mid))?.into_iter().next();
            self.emit(EventKind::MachineLost, None, m)?;
        }
        Ok(())
    }

    // ------------------------------------------------------------------ frames

    fn on_frame(&mut self, mid: &str, frame: NodeToHub) -> Result<()> {
        match frame {
            NodeToHub::Hello { .. } | NodeToHub::Enroll { .. } | NodeToHub::Pong { .. } => {}
            NodeToHub::Ping { n } => self.send(mid, HubToNode::Pong { n }),
            NodeToHub::MachineInfo { reported } => {
                self.db.execute("UPDATE machines SET reported=?1 WHERE machine_id=?2", params![serde_json::to_string(&reported)?, mid])?;
                let m = self.machine_records(Some(mid))?.into_iter().next();
                self.emit(EventKind::MachineUpdated, None, m)?;
            }
            NodeToHub::Resync { profiles, attached, lifecycle, last_seq: _ } => self.resync(mid, profiles, attached, lifecycle)?,
            NodeToHub::ProfileUpsert { profile } => match self.upsert_profile(mid, &profile) {
                Err(e) => self.send(mid, HubToNode::Error { code: "PROFILE_REJECTED".into(), msg: e.to_string() }),
                Ok(true) => self.emit_agent(EventKind::Updated, &profile.agent_id)?,
                Ok(false) => {}
            },
            NodeToHub::AgentUp { session_id, profile } => {
                if let Err(e) = self.upsert_profile(mid, &profile) {
                    self.send(mid, HubToNode::Error { code: "AGENT_REJECTED".into(), msg: format!("{}: {e}", profile.agent_id) });
                    return Ok(());
                }
                let fence: i64 = self.db.query_row("SELECT fence FROM agents WHERE agent_id=?1", params![profile.agent_id], |r| r.get(0))?;
                let fence = fence as u64 + 1;
                self.db.execute(
                    "UPDATE agents SET state='online', readiness='idle', session_id=?1, fence=?2, since=?3, end_reason=NULL WHERE agent_id=?4",
                    params![session_id, fence as i64, now().to_rfc3339(), profile.agent_id],
                )?;
                self.send(mid, HubToNode::AgentUpAck { agent_id: profile.agent_id.clone(), session_id, fence });
                self.emit_agent(EventKind::Joined, &profile.agent_id)?;
            }
            NodeToHub::AgentDown { agent_id, session_id, fence, reason } => {
                let cur: Option<(String, Option<String>, i64)> = self
                    .db
                    .query_row("SELECT machine_id, session_id, fence FROM agents WHERE agent_id=?1", params![agent_id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                    .optional()?;
                let ok = cur.is_some_and(|(m, s, f)| m == mid && s.as_deref() == Some(session_id.as_str()) && f as u64 == fence);
                if !ok {
                    debug!("hub: ignoring stale AgentDown for {agent_id}");
                    return Ok(());
                }
                self.db.execute(
                    "UPDATE agents SET state='offline', readiness='detached', session_id=NULL, end_reason=?1 WHERE agent_id=?2",
                    params![s(&reason), agent_id],
                )?;
                self.emit_agent(EventKind::Left, &agent_id)?;
            }
            NodeToHub::Submit { agent_id, envelope, local_recipients, exclude, .. } => {
                let msg_id = envelope.id.clone();
                match self.submit(mid, &agent_id, envelope, &local_recipients, &exclude) {
                    Ok((count, recipients)) => self.send(mid, HubToNode::Accepted { msg_id, remote: count, recipients }),
                    Err(SubmitErr::Reject(code, msg)) => self.send(mid, HubToNode::Rejected { msg_id, code: code.into(), msg }),
                    // Storage failure: roll back and stay silent; the node keeps the row and retries.
                    Err(SubmitErr::Db(e)) => return Err(e),
                }
            }
            NodeToHub::Receipt { msg_id, recipient, state } => {
                let cur: Option<(String, String)> = self
                    .db
                    .query_row("SELECT machine_id, state FROM deliveries WHERE msg_id=?1 AND recipient=?2", params![msg_id, recipient], |r| Ok((r.get(0)?, r.get(1)?)))
                    .optional()?;
                if let Some((m, st)) = cur {
                    if m == mid {
                        if state_rank(state) > state_rank(p(&st)) {
                            self.db.execute(
                                "UPDATE deliveries SET state=?1, updated=?2 WHERE msg_id=?3 AND recipient=?4",
                                params![s(&state), now().to_rfc3339(), msg_id, recipient],
                            )?;
                        }
                        if state_rank(state) >= state_rank(DeliveryState::Stored) {
                            if let Some(c) = self.conns.get_mut(mid) {
                                c.inflight.remove(&(msg_id.clone(), recipient.clone()));
                            }
                            self.pump_after.insert(mid.to_string());
                        }
                    }
                }
                self.send(mid, HubToNode::ReceiptAck { msg_id, recipient, state });
            }
            NodeToHub::Journal { entry } => {
                self.db.execute(
                    "INSERT OR IGNORE INTO journal(msg_id, machine_id, body) VALUES(?1,?2,?3)",
                    params![entry.msg_id, mid, serde_json::to_string(&entry)?],
                )?;
                self.send(mid, HubToNode::JournalAck { msg_id: entry.msg_id });
            }
        }
        Ok(())
    }

    fn resync(
        &mut self,
        mid: &str,
        profiles: Vec<AgentProfile>,
        attached: Vec<agentlan_proto::wire::AttachedSession>,
        lifecycle: Vec<agentlan_proto::wire::SessionLifecycle>,
    ) -> Result<()> {
        let mut outcomes = Vec::new();
        for prof in &profiles {
            match self.upsert_profile(mid, prof) {
                Ok(true) => self.emit_agent(EventKind::Updated, &prof.agent_id)?,
                Ok(_) => {}
                Err(e) => outcomes.push(SessionOutcome {
                    agent_id: prof.agent_id.clone(),
                    session_id: String::new(),
                    outcome: "rejected".into(),
                    fence: None,
                    reason: Some(e.to_string()),
                }),
            }
        }
        for l in &lifecycle {
            debug!("hub: offline session {} for {} ({:?})", l.session_id, l.agent_id, l.reason);
        }
        // Agents of this machine that were online/lost but are no longer attached: they left.
        let attached_ids: Vec<&str> = attached.iter().map(|a| a.agent_id.as_str()).collect();
        for a in self.agent_records("WHERE a.machine_id=?1 AND a.state IN ('online','lost')", Some(mid))? {
            if !attached_ids.contains(&a.profile.agent_id.as_str()) {
                self.db.execute(
                    "UPDATE agents SET state='offline', readiness='detached', session_id=NULL, end_reason='left' WHERE agent_id=?1",
                    params![a.profile.agent_id],
                )?;
                self.emit_agent(EventKind::Left, &a.profile.agent_id)?;
            }
        }
        for sess in &attached {
            let row: Option<(String, String, Option<String>, i64, i64)> = self
                .db
                .query_row(
                    "SELECT machine_id, state, session_id, fence, retired FROM agents WHERE agent_id=?1",
                    params![sess.agent_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
                )
                .optional()?;
            let reject = |why: &str| SessionOutcome {
                agent_id: sess.agent_id.clone(),
                session_id: sess.session_id.clone(),
                outcome: "rejected".into(),
                fence: None,
                reason: Some(why.into()),
            };
            let Some((owner, state, cur_sess, fence, retired)) = row else {
                outcomes.push(reject("unknown agent"));
                continue;
            };
            if owner != mid {
                outcomes.push(reject("not owned by this machine"));
                continue;
            }
            if retired != 0 {
                outcomes.push(reject("retired"));
                continue;
            }
            let fence = fence as u64 + 1;
            let continued = state == "online" && cur_sess.as_deref() == Some(sess.session_id.as_str());
            self.db.execute(
                "UPDATE agents SET state='online', readiness=CASE WHEN readiness='detached' THEN 'idle' ELSE readiness END,
                 session_id=?1, fence=?2, since=COALESCE(CASE WHEN session_id=?1 THEN since END, ?3), end_reason=NULL WHERE agent_id=?4",
                params![sess.session_id, fence as i64, sess.since.to_rfc3339(), sess.agent_id],
            )?;
            if !continued {
                self.emit_agent(EventKind::Joined, &sess.agent_id)?;
            }
            outcomes.push(SessionOutcome {
                agent_id: sess.agent_id.clone(),
                session_id: sess.session_id.clone(),
                outcome: if continued { "continued" } else { "registered" }.into(),
                fence: Some(fence),
                reason: None,
            });
        }
        let was: String = self.db.query_row("SELECT state FROM machines WHERE machine_id=?1", params![mid], |r| r.get(0))?;
        self.db.execute("UPDATE machines SET state='online' WHERE machine_id=?1", params![mid])?;
        if was != "online" {
            let m = self.machine_records(Some(mid))?.into_iter().next();
            self.emit(EventKind::MachineOnline, None, m)?;
        }
        self.deadlines.remove(mid);
        self.send(mid, HubToNode::ResyncAck { outcomes });
        // Snapshot at the current sequence (covers every event emitted above), paged.
        let seq = meta_u64(&self.db, "seq")?;
        let machines = self.machine_records(None)?;
        let agents = self.agent_records("", None)?;
        let pages: Vec<Vec<AgentRecord>> = if agents.is_empty() { vec![vec![]] } else { agents.chunks(SNAPSHOT_PAGE).map(|c| c.to_vec()).collect() };
        let n = pages.len();
        for (i, page) in pages.into_iter().enumerate() {
            let ms = if i == 0 { machines.clone() } else { vec![] };
            self.send(mid, HubToNode::Snapshot { seq, machines: ms, agents: page, more: i + 1 < n });
        }
        // Deliveries flow only now, oldest first.
        if let Some(c) = self.conns.get_mut(mid) {
            c.ready = true;
            c.inflight.clear();
        }
        self.pump_after.insert(mid.to_string());
        Ok(())
    }

    /// Returns true if the stored profile changed.
    fn upsert_profile(&mut self, mid: &str, prof: &AgentProfile) -> Result<bool> {
        let row: Option<(String, i64, String)> = self
            .db
            .query_row("SELECT machine_id, retired, profile FROM agents WHERE agent_id=?1", params![prof.agent_id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .optional()?;
        let json = serde_json::to_string(prof)?;
        match row {
            Some((owner, _, _)) if owner != mid => anyhow::bail!("agent {} belongs to another machine", prof.agent_id),
            Some((_, retired, _)) if retired != 0 => anyhow::bail!("agent {} is retired", prof.agent_id),
            Some((_, _, cur)) if cur == json => Ok(false),
            Some(_) => {
                self.db.execute("UPDATE agents SET name=?1, profile=?2 WHERE agent_id=?3", params![prof.name, json, prof.agent_id])?;
                Ok(true)
            }
            None => {
                self.db.execute(
                    "INSERT INTO agents(agent_id, machine_id, name, profile, state, readiness) VALUES(?1,?2,?3,?4,'offline','detached')",
                    params![prof.agent_id, mid, prof.name, json],
                )?;
                Ok(true)
            }
        }
    }

    /// Commit a submission (inside the current transaction) and schedule deliveries.
    /// Submits are durable traffic: they use the node-authenticated path; fences guard
    /// live session mutations (AgentDown) instead.
    fn submit(&mut self, mid: &str, agent_id: &str, mut env: Amf, local_recipients: &[String], exclude: &[String]) -> Result<(u32, Vec<String>), SubmitErr> {
        let sender = self.agent(agent_id)?.ok_or_else(|| SubmitErr::Reject("UNKNOWN_SENDER", agent_id.into()))?;
        if sender.machine_id != mid {
            return Err(SubmitErr::Reject("NOT_OWNER", format!("{agent_id} does not belong to this machine")));
        }
        if sender.state == PresenceState::Retired {
            return Err(SubmitErr::Reject("RETIRED", agent_id.into()));
        }
        // Dedup is scoped to the sender: a retry returns the original result; reuse by
        // another sender is a conflict, never a silent success.
        let prev: Option<(String, i64)> =
            self.db.query_row("SELECT from_agent, remote_count FROM messages WHERE msg_id=?1", params![env.id], |r| Ok((r.get(0)?, r.get(1)?))).optional()?;
        if let Some((from, count)) = prev {
            if from == agent_id {
                let mut st = self.db.prepare("SELECT recipient FROM deliveries WHERE msg_id=?1 ORDER BY rowid")?;
                let r: Vec<String> = st.query_map(params![env.id], |row| row.get(0))?.collect::<Result<_, _>>()?;
                return Ok((count as u32, r));
            }
            return Err(SubmitErr::Reject("ID_CONFLICT", format!("message id {} was already used by another sender", env.id)));
        }
        env.from.agent = sender.profile.agent_id.clone();
        env.from.handle = Some(sender.handle.clone());
        env.from.role = Some(sender.profile.role.clone());
        env.from.origin = Origin::Agent;
        env.validate().map_err(|e| SubmitErr::Reject("INVALID", e.to_string()))?;

        let all = self.agent_records("WHERE a.retired=0", None)?;
        let remote = |a: &&AgentRecord| a.machine_id != mid && a.profile.agent_id != agent_id;
        let online = |a: &&AgentRecord| a.state == PresenceState::Online;
        let project_ok = |a: &AgentRecord, pr: &Option<String>| pr.as_ref().is_none_or(|p| a.profile.project.as_deref() == Some(p.as_str()));
        let mut recips: Vec<AgentRecord> = match &env.to {
            Target::Agent { agent } => all.iter().filter(remote).filter(|a| &a.profile.agent_id == agent).cloned().collect(),
            Target::Handle { handle } => all.iter().filter(remote).filter(|a| a.handle == handle.trim_start_matches('@')).cloned().collect(),
            Target::Role { role, project, .. } => all.iter().filter(remote).filter(online).filter(|a| &a.profile.role == role && project_ok(a, project)).cloned().collect(),
            Target::Project { project, .. } => all.iter().filter(remote).filter(online).filter(|a| a.profile.project.as_deref() == Some(project.as_str())).cloned().collect(),
            Target::Machine { machine } => {
                all.iter().filter(remote).filter(online).filter(|a| &a.machine_id == machine || a.handle.ends_with(&format!("@{machine}"))).cloned().collect()
            }
            Target::All => all.iter().filter(remote).filter(online).cloned().collect(),
        };
        // Loop guard: the sender's node has paused these pairs.
        recips.retain(|a| !exclude.contains(&a.profile.agent_id));
        if matches!(&env.to, Target::Role { pick: Pick::One, .. } | Target::Project { pick: Pick::One, .. }) {
            if !local_recipients.is_empty() {
                recips.clear();
            } else if !recips.is_empty() {
                let i = recips.iter().position(|a| a.readiness == Readiness::Idle).unwrap_or(0);
                recips = vec![recips.swap_remove(i)];
            } else {
                // Nobody live: a wake-enabled worker whose node is connected can take it
                // (its node starts it). Broadcasts never wake anyone.
                let wanted = |a: &AgentRecord| match &env.to {
                    Target::Role { role, project, .. } => &a.profile.role == role && project_ok(a, project),
                    Target::Project { project, .. } => a.profile.project.as_deref() == Some(project.as_str()),
                    _ => false,
                };
                recips = all
                    .iter()
                    .filter(remote)
                    .filter(|a| wanted(a) && a.profile.capabilities.iter().any(|c| c == "wake") && !exclude.contains(&a.profile.agent_id) && self.conns.contains_key(&a.machine_id))
                    .take(1)
                    .cloned()
                    .collect();
            }
        }
        if env.to.is_direct() && recips.is_empty() && local_recipients.is_empty() {
            return Err(SubmitErr::Reject("UNKNOWN_RECIPIENT", format!("no agent matches {:?}", env.to)));
        }
        if !env.to.is_direct() && recips.len() + local_recipients.len() > FANOUT_CAP {
            return Err(SubmitErr::Reject("FANOUT_CAP", format!("{} recipients exceeds the cap of {FANOUT_CAP}", recips.len() + local_recipients.len())));
        }

        self.db.execute(
            "INSERT INTO messages(msg_id, from_agent, origin_machine, envelope, remote_count, created_at, expires_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![env.id, agent_id, mid, serde_json::to_string(&env)?, recips.len() as i64, env.created_at.to_rfc3339(), env.expires_at.to_rfc3339()],
        )?;
        for r in &recips {
            let seq: i64 = self.db.query_row(
                "INSERT INTO pair_seq(sender, recipient, next) VALUES(?1,?2,1)
                 ON CONFLICT(sender, recipient) DO UPDATE SET next=next+1 RETURNING next",
                params![agent_id, r.profile.agent_id],
                |row| row.get(0),
            )?;
            self.db.execute(
                "INSERT INTO deliveries(msg_id, recipient, machine_id, pair_seq, state, updated) VALUES(?1,?2,?3,?4,'accepted',?5)",
                params![env.id, r.profile.agent_id, r.machine_id, seq, now().to_rfc3339()],
            )?;
            self.pump_after.insert(r.machine_id.clone());
        }
        Ok((recips.len() as u32, recips.iter().map(|r| r.profile.agent_id.clone()).collect()))
    }
}
