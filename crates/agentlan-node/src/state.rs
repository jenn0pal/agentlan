//! Single-owner node state: sessions, local router, inbox/outbox, roster cache.
//! Runs on one dedicated thread; every database write happens here.
//!
//! All durable traffic to the hub (submits, journal entries, receipts) goes through
//! one ordered outbox pump; rows stay until the hub confirms them.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use agentlan_proto::amf::{Amf, From, Origin, Pick, SendDraft, Target};
use agentlan_proto::ipc::{IpcError, IpcOp, IpcOut, IpcPush, IpcRequest, IpcResponse, RemoteResult, SendResult};
use agentlan_proto::records::{AgentProfile, AgentRecord, EndReason, MachineRecord, MachineReported, PresenceState, Readiness, RosterEntry};
use agentlan_proto::wire::{AttachedSession, DeliveryState, EventKind, HubToNode, JournalEntry, NodeToHub};
use agentlan_proto::{DEFAULT_TTL_HOURS, MAX_FRAME, Ts, now};
use agentlan_store::rusqlite::{Connection, OptionalExtension, params};
use anyhow::Result;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::NodeFile;
use crate::link::LinkCtl;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS messages(msg_id TEXT PRIMARY KEY, envelope TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS inbox(
  agent_id TEXT NOT NULL, msg_id TEXT NOT NULL, state TEXT NOT NULL, remote INTEGER NOT NULL,
  received_at TEXT NOT NULL, PRIMARY KEY(agent_id, msg_id));
CREATE TABLE IF NOT EXISTS sent(msg_id TEXT PRIMARY KEY, sender TEXT NOT NULL, result TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS outbox(id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, msg_id TEXT NOT NULL, body TEXT NOT NULL);
"#;

/// How long `send` waits for the hub's verdict before answering "queued".
const SEND_WAIT: Duration = Duration::from_secs(3);
/// Max outbox rows in flight (sent, not yet confirmed) on one link.
const WINDOW: usize = 256;
/// Resend an unconfirmed outbox row after this long.
const RESEND: Duration = Duration::from_secs(10);
/// Headroom reserved for the Submit frame around the envelope.
const FRAME_HEADROOM: usize = 8 * 1024;

pub enum NCmd {
    IpcOpen { conn: u64, tx: mpsc::UnboundedSender<IpcOut> },
    IpcClose { conn: u64 },
    Ipc { conn: u64, req: IpcRequest },
    HubUp { tx: mpsc::Sender<NodeToHub> },
    HubFrame(HubToNode),
    HubDown,
    Tick,
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum HubStatus {
    Offline,
    Resyncing,
    Ready,
}

impl HubStatus {
    fn as_str(self) -> &'static str {
        match self {
            HubStatus::Offline => "offline",
            HubStatus::Resyncing => "resyncing",
            HubStatus::Ready => "ready",
        }
    }
}

struct Session {
    session_id: String,
    conn: u64,
    since: Ts,
    fence: Option<u64>,
}

struct Conn {
    tx: mpsc::UnboundedSender<IpcOut>,
    attached: Option<String>,
    /// None = not watching; Some(None) = all local agents; Some(Some(id)) = one agent.
    watch: Option<Option<String>>,
}

struct Waiter {
    conn: u64,
    req: u64,
    agent: String,
    deadline: Instant,
    /// Only a reply to this message satisfies the waiter (request/reply).
    in_reply_to: Option<String>,
}

struct PendingSend {
    conn: u64,
    req: u64,
    result: SendResult,
    deadline: Instant,
}

pub struct NodeState {
    home: PathBuf,
    db: Connection,
    cfg: NodeFile,
    reported: MachineReported,
    profiles: Vec<AgentProfile>,
    hub: HubStatus,
    hub_tx: Option<mpsc::Sender<NodeToHub>>,
    link: mpsc::UnboundedSender<LinkCtl>,
    base_seq: u64,
    /// False from HubUp until the (last page of the) resync Snapshot arrives.
    have_snapshot: bool,
    snap_stage: Option<(u64, Vec<MachineRecord>, Vec<AgentRecord>)>,
    base_agents: HashMap<String, AgentRecord>,
    base_machines: HashMap<String, MachineRecord>,
    sessions: HashMap<String, Session>,
    /// Sessions that ended before their fence arrived: session_id → (agent, reason).
    ended: HashMap<String, (String, EndReason)>,
    conns: HashMap<u64, Conn>,
    waiters: Vec<Waiter>,
    pending: HashMap<String, PendingSend>,
    /// Outbox row ids sent on the current link, with send time.
    inflight: HashMap<i64, Instant>,
}

fn ok(id: u64, v: Value) -> IpcOut {
    IpcOut::Response(IpcResponse { id, ok: true, result: Some(v), error: None })
}

fn fail(id: u64, code: &str, msg: impl Into<String>) -> IpcOut {
    IpcOut::Response(IpcResponse { id, ok: false, result: None, error: Some(IpcError { code: code.into(), msg: msg.into() }) })
}

struct IpcErr(&'static str, String);

impl<E: std::fmt::Display> std::convert::From<E> for IpcErr {
    fn from(e: E) -> Self {
        IpcErr("INTERNAL", e.to_string())
    }
}

type R<T> = std::result::Result<T, IpcErr>;

fn ds(s: DeliveryState) -> &'static str {
    match s {
        DeliveryState::Accepted => "accepted",
        DeliveryState::Stored => "stored",
        DeliveryState::Surfaced => "surfaced",
        DeliveryState::Acked => "acked",
        DeliveryState::Expired => "expired",
    }
}

fn rank(s: &str) -> u8 {
    match s {
        "accepted" => 0,
        "stored" => 1,
        "surfaced" => 2,
        "acked" => 3,
        "expired" => 4,
        _ => 0,
    }
}

/// Wire id for a caller-supplied retry key: scoped to the sender, so two agents can
/// never collide on the same key.
pub fn canonical_msg_id(sender: &str, client_id: &str) -> String {
    let d = Sha256::digest(format!("{sender}\n{client_id}").as_bytes());
    format!("msg_{}", &data_encoding::BASE32_NOPAD.encode(&d)[..26])
}

impl NodeState {
    pub fn new(home: PathBuf, db: Connection, cfg: NodeFile, reported: MachineReported, link: mpsc::UnboundedSender<LinkCtl>) -> Result<NodeState> {
        agentlan_store::migrate(&db, SCHEMA)?;
        let profiles = crate::load_profiles(&home)?.agents;
        Ok(NodeState {
            home,
            db,
            cfg,
            reported,
            profiles,
            hub: HubStatus::Offline,
            hub_tx: None,
            link,
            base_seq: 0,
            have_snapshot: false,
            snap_stage: None,
            base_agents: HashMap::new(),
            base_machines: HashMap::new(),
            sessions: HashMap::new(),
            ended: HashMap::new(),
            conns: HashMap::new(),
            waiters: Vec::new(),
            pending: HashMap::new(),
            inflight: HashMap::new(),
        })
    }

    pub fn run(mut self, mut rx: mpsc::UnboundedReceiver<NCmd>) {
        while let Some(c) = rx.blocking_recv() {
            if let Err(e) = self.handle(c) {
                warn!("node: command failed: {e:#}");
            }
        }
    }

    fn handle(&mut self, c: NCmd) -> Result<()> {
        match c {
            NCmd::IpcOpen { conn, tx } => {
                self.conns.insert(conn, Conn { tx, attached: None, watch: None });
            }
            NCmd::IpcClose { conn } => {
                if let Some(agent) = self.conns.get(&conn).and_then(|c| c.attached.clone()) {
                    // Connection dropped without Detach: the runner/adapter went away.
                    self.end_session(&agent, EndReason::Detached);
                }
                self.conns.remove(&conn);
                self.waiters.retain(|w| w.conn != conn);
                self.pending.retain(|_, p| p.conn != conn);
            }
            NCmd::Ipc { conn, req } => {
                let id = req.id;
                let out = match self.ipc(conn, req) {
                    Ok(Some(v)) => Some(ok(id, v)),
                    Ok(None) => None, // answered later (wait / pending send)
                    Err(IpcErr(code, msg)) => Some(fail(id, code, msg)),
                };
                if let Some(o) = out {
                    self.reply(conn, o);
                }
            }
            NCmd::HubUp { tx } => self.hub_up(tx)?,
            NCmd::HubFrame(f) => self.hub_frame(f)?,
            NCmd::HubDown => {
                self.hub = HubStatus::Offline;
                self.hub_tx = None;
                self.inflight.clear();
                for s in self.sessions.values_mut() {
                    s.fence = None;
                }
                let pend: Vec<String> = self.pending.keys().cloned().collect();
                for m in pend {
                    self.finish_pending(&m, RemoteResult::Queued);
                }
                self.push_all("hub", json!({"status": "offline"}));
            }
            NCmd::Tick => {
                self.tick();
                self.pump()?;
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------ helpers

    fn reply(&self, conn: u64, out: IpcOut) {
        if let Some(c) = self.conns.get(&conn) {
            let _ = c.tx.send(out);
        }
    }

    fn push(&self, conn: u64, event: &str, data: Value) {
        self.reply(conn, IpcOut::Push(IpcPush { event: event.into(), data }));
    }

    fn push_all(&self, event: &str, data: Value) {
        for (id, c) in &self.conns {
            if c.watch.is_some() || c.attached.is_some() {
                self.push(*id, event, data.clone());
            }
        }
    }

    fn reload_profiles(&mut self) {
        match crate::load_profiles(&self.home) {
            Ok(pf) => self.profiles = pf.agents,
            Err(e) => warn!("node: cannot read profiles: {e:#}"),
        }
    }

    fn handle_of(&self, p: &AgentProfile) -> String {
        agentlan_proto::ids::handle(&p.name, &self.cfg.alias)
    }

    fn find_local(&self, key: &str) -> Option<AgentProfile> {
        let key = key.trim_start_matches('@');
        let name = match agentlan_proto::ids::split_handle(key) {
            Some((n, a)) if a == self.cfg.alias => n,
            Some(_) => return None,
            None => key,
        };
        self.profiles.iter().find(|p| p.agent_id == key || p.name == name).cloned()
    }

    fn agent_for(&mut self, conn: u64, explicit: Option<&str>) -> R<AgentProfile> {
        self.reload_profiles();
        let key = match explicit {
            Some(k) => k.to_string(),
            None => self.conns.get(&conn).and_then(|c| c.attached.clone()).ok_or(IpcErr("NOT_ATTACHED", "no agent given and this connection is not attached".into()))?,
        };
        self.find_local(&key).ok_or(IpcErr("UNKNOWN_AGENT", format!("no local agent '{key}' (see `agentlan agent ls`)")))
    }

    /// Live (non-durable) frames: presence and heartbeats. Durable frames use the outbox.
    fn send_live(&self, f: NodeToHub) -> bool {
        match (&self.hub_tx, self.hub) {
            (Some(tx), HubStatus::Ready | HubStatus::Resyncing) => {
                let ok = tx.try_send(f).is_ok();
                if !ok {
                    // A presence frame could not be queued: a resync reconciles everything.
                    warn!("node: hub link congested; forcing a resync");
                    let _ = self.link.send(LinkCtl::Reconnect);
                }
                ok
            }
            _ => false,
        }
    }

    /// Hub view is complete only after resync *and* the full snapshot.
    fn hub_label(&self) -> &'static str {
        if self.hub == HubStatus::Ready && !self.have_snapshot { "resyncing" } else { self.hub.as_str() }
    }

    fn local_record(&self, p: &AgentProfile) -> AgentRecord {
        let s = self.sessions.get(&p.agent_id);
        AgentRecord {
            profile: p.clone(),
            machine_id: self.cfg.machine_id.clone(),
            handle: self.handle_of(p),
            state: if s.is_some() { PresenceState::Online } else { PresenceState::Offline },
            readiness: if s.is_some() { Readiness::Idle } else { Readiness::Detached },
            session_id: s.map(|s| s.session_id.clone()),
            since: s.map(|s| s.since),
            end_reason: None,
        }
    }

    fn roster(&self, project: Option<&str>, role: Option<&str>) -> Vec<RosterEntry> {
        let stale = self.hub != HubStatus::Ready || !self.have_snapshot;
        let mut out: Vec<RosterEntry> = self.profiles.iter().map(|p| RosterEntry { agent: self.local_record(p), same_machine: true, stale: false }).collect();
        let mut remote: Vec<&AgentRecord> = self.base_agents.values().filter(|a| a.machine_id != self.cfg.machine_id).collect();
        remote.sort_by(|a, b| a.handle.cmp(&b.handle));
        out.extend(remote.into_iter().map(|a| RosterEntry { agent: a.clone(), same_machine: false, stale }));
        out.retain(|e| {
            project.is_none_or(|p| e.agent.profile.project.as_deref() == Some(p)) && role.is_none_or(|r| e.agent.profile.role == r) && e.agent.state != PresenceState::Retired
        });
        out
    }

    // ------------------------------------------------------------------ outbox pump

    /// Send outbox rows in order, bounded by WINDOW; resend rows unconfirmed for RESEND.
    fn pump(&mut self) -> Result<()> {
        if self.hub != HubStatus::Ready || self.hub_tx.is_none() {
            return Ok(());
        }
        let now_i = Instant::now();
        let mut st = self.db.prepare("SELECT id, kind, msg_id, body FROM outbox ORDER BY id LIMIT ?1")?;
        let rows: Vec<(i64, String, String, String)> =
            st.query_map(params![(WINDOW * 2) as i64], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?.collect::<Result<_, _>>()?;
        drop(st);
        for (id, kind, msg_id, body) in rows {
            if let Some(t) = self.inflight.get(&id) {
                if now_i.duration_since(*t) < RESEND {
                    continue;
                }
            } else if self.inflight.len() >= WINDOW {
                break;
            }
            let frame = match kind.as_str() {
                "submit" => {
                    let v: Value = serde_json::from_str(&body)?;
                    let envelope: Amf = serde_json::from_value(v["envelope"].clone())?;
                    if serde_json::to_vec(&envelope)?.len() + FRAME_HEADROOM > MAX_FRAME {
                        // Can never be sent: give it a terminal outcome so later rows drain.
                        self.settle_submit(&msg_id, RemoteResult::Rejected { code: "TOO_LARGE".into(), msg: "message exceeds the frame limit".into() })?;
                        continue;
                    }
                    NodeToHub::Submit {
                        agent_id: v["agentId"].as_str().unwrap_or_default().to_string(),
                        session_id: None,
                        fence: None,
                        envelope,
                        local_recipients: serde_json::from_value(v["localRecipients"].clone()).unwrap_or_default(),
                    }
                }
                "journal" => NodeToHub::Journal { entry: serde_json::from_str(&body)? },
                "receipt" => {
                    let v: Value = serde_json::from_str(&body)?;
                    NodeToHub::Receipt {
                        msg_id: msg_id.clone(),
                        recipient: v["recipient"].as_str().unwrap_or_default().to_string(),
                        state: serde_json::from_value(v["state"].clone())?,
                    }
                }
                _ => continue,
            };
            let sent = self.hub_tx.as_ref().is_some_and(|tx| tx.try_send(frame).is_ok());
            if !sent {
                break; // writer busy; resume on the next tick
            }
            self.inflight.insert(id, now_i);
        }
        Ok(())
    }

    fn outbox_done(&mut self, sql_where: &str, args: &[&dyn agentlan_store::rusqlite::ToSql]) -> Result<()> {
        let mut st = self.db.prepare(&format!("SELECT id FROM outbox WHERE {sql_where}"))?;
        let ids: Vec<i64> = st.query_map(args, |r| r.get(0))?.collect::<Result<_, _>>()?;
        drop(st);
        self.db.execute(&format!("DELETE FROM outbox WHERE {sql_where}"), args)?;
        for id in ids {
            self.inflight.remove(&id);
        }
        Ok(())
    }

    /// Terminal outcome for a submit: update the cached result and drop the row together.
    fn settle_submit(&mut self, msg_id: &str, remote: RemoteResult) -> Result<()> {
        self.db.execute_batch("BEGIN IMMEDIATE")?;
        let r = (|| -> Result<()> {
            let cur: Option<String> = self.db.query_row("SELECT result FROM sent WHERE msg_id=?1", params![msg_id], |r| r.get(0)).optional()?;
            if let Some(c) = cur {
                let mut res: SendResult = serde_json::from_str(&c)?;
                res.remote = remote.clone();
                self.db.execute("UPDATE sent SET result=?1 WHERE msg_id=?2", params![serde_json::to_string(&res)?, msg_id])?;
            }
            self.outbox_done("kind='submit' AND msg_id=?1", &[&msg_id])
        })();
        match r {
            Ok(()) => self.db.execute_batch("COMMIT")?,
            Err(e) => {
                let _ = self.db.execute_batch("ROLLBACK");
                return Err(e);
            }
        }
        self.finish_pending(msg_id, remote);
        Ok(())
    }

    // ------------------------------------------------------------------ hub link

    fn hub_up(&mut self, tx: mpsc::Sender<NodeToHub>) -> Result<()> {
        self.hub_tx = Some(tx);
        self.hub = HubStatus::Resyncing;
        self.have_snapshot = false;
        self.snap_stage = None;
        self.inflight.clear();
        // Sessions that ended while the hub was away are covered by resync (not attached ⇒ left).
        self.ended.clear();
        self.reload_profiles();
        self.send_live(NodeToHub::MachineInfo { reported: self.reported.clone() });
        let attached = self
            .sessions
            .iter()
            .map(|(a, s)| AttachedSession { agent_id: a.clone(), session_id: s.session_id.clone(), since: s.since })
            .collect();
        self.send_live(NodeToHub::Resync { profiles: self.profiles.clone(), attached, lifecycle: vec![], last_seq: self.base_seq });
        Ok(())
    }

    fn hub_frame(&mut self, f: HubToNode) -> Result<()> {
        match f {
            HubToNode::ResyncAck { outcomes } => {
                let mut mentioned: HashSet<String> = HashSet::new();
                for o in outcomes {
                    mentioned.insert(o.session_id.clone());
                    let current = self.sessions.get(&o.agent_id).is_some_and(|s| s.session_id == o.session_id);
                    if o.outcome == "rejected" {
                        if current {
                            let conn = self.sessions.get(&o.agent_id).map(|s| s.conn);
                            self.sessions.remove(&o.agent_id);
                            self.cancel_waiters(conn, &o.agent_id);
                            if let Some(c) = conn {
                                if let Some(cc) = self.conns.get_mut(&c) {
                                    cc.attached = None;
                                }
                                self.push(c, "session", json!({"status": "rejected", "agentId": o.agent_id, "reason": o.reason}));
                            }
                        }
                        continue;
                    }
                    if current {
                        if let Some(s) = self.sessions.get_mut(&o.agent_id) {
                            s.fence = o.fence;
                        }
                    } else if let (Some((agent, reason)), Some(fence)) = (self.ended.remove(&o.session_id), o.fence) {
                        // Ended before its fence arrived: close it on the hub now.
                        self.send_live(NodeToHub::AgentDown { agent_id: agent, session_id: o.session_id.clone(), fence, reason });
                    }
                }
                // Sessions attached after the resync payload was captured: register them now.
                let late: Vec<(String, String)> = self
                    .sessions
                    .iter()
                    .filter(|(_, s)| s.fence.is_none() && !mentioned.contains(&s.session_id))
                    .map(|(a, s)| (a.clone(), s.session_id.clone()))
                    .collect();
                self.hub = HubStatus::Ready;
                for (agent, sid) in late {
                    if let Some(p) = self.profiles.iter().find(|p| p.agent_id == agent).cloned() {
                        self.send_live(NodeToHub::AgentUp { session_id: sid, profile: p });
                    }
                }
                self.pump()?;
            }
            HubToNode::Snapshot { seq, machines, agents, more } => {
                let stage = self.snap_stage.get_or_insert_with(|| (seq, vec![], vec![]));
                stage.1.extend(machines);
                stage.2.extend(agents);
                if !more {
                    let (seq, machines, agents) = self.snap_stage.take().unwrap();
                    self.base_seq = seq;
                    self.have_snapshot = true;
                    self.base_machines = machines.into_iter().map(|m| (m.machine_id.clone(), m)).collect();
                    self.base_agents = agents.into_iter().map(|a| (a.profile.agent_id.clone(), a)).collect();
                    self.push_all("hub", json!({"status": "ready"}));
                }
            }
            HubToNode::Event { event } => {
                if !self.have_snapshot || event.seq <= self.base_seq {
                    return Ok(()); // covered by the snapshot / already applied
                }
                if event.seq != self.base_seq + 1 {
                    warn!("node: roster gap (have {}, got {}); resynchronising", self.base_seq, event.seq);
                    let _ = self.link.send(LinkCtl::Reconnect);
                    return Ok(());
                }
                self.base_seq = event.seq;
                if let Some(m) = &event.machine {
                    self.base_machines.insert(m.machine_id.clone(), m.clone());
                }
                if let Some(a) = &event.agent {
                    self.base_agents.insert(a.profile.agent_id.clone(), a.clone());
                    // Our own agents were already announced locally (no LAN); don't repeat.
                    if a.machine_id != self.cfg.machine_id {
                        self.push_all("roster", json!({"kind": event.kind, "seq": event.seq, "agent": a, "sameMachine": false}));
                    }
                } else if matches!(event.kind, EventKind::MachineLost | EventKind::MachineOnline) {
                    self.push_all("roster", json!({"kind": event.kind, "seq": event.seq, "machine": event.machine}));
                }
            }
            HubToNode::AgentUpAck { agent_id, session_id, fence } => {
                let current = self.sessions.get(&agent_id).is_some_and(|s| s.session_id == session_id);
                if current {
                    if let Some(s) = self.sessions.get_mut(&agent_id) {
                        s.fence = Some(fence);
                    }
                } else if let Some((agent, reason)) = self.ended.remove(&session_id) {
                    self.send_live(NodeToHub::AgentDown { agent_id: agent, session_id, fence, reason });
                }
            }
            HubToNode::Accepted { msg_id, remote } => self.settle_submit(&msg_id, RemoteResult::Accepted { count: remote })?,
            HubToNode::Rejected { msg_id, code, msg } => self.settle_submit(&msg_id, RemoteResult::Rejected { code, msg })?,
            HubToNode::Deliver { recipient, pair_seq, envelope } => {
                if !self.profiles.iter().any(|p| p.agent_id == recipient) {
                    self.reload_profiles();
                }
                if !self.profiles.iter().any(|p| p.agent_id == recipient) {
                    // Profile deleted: settle the delivery so it doesn't block the hub's window.
                    warn!("node: delivery for unknown local agent {recipient}; marking expired");
                    self.queue_receipt(&envelope.id, &recipient, DeliveryState::Expired)?;
                    return self.pump();
                }
                // An existing envelope with this id from a different sender is a conflict.
                let existing: Option<String> = self.db.query_row("SELECT envelope FROM messages WHERE msg_id=?1", params![envelope.id], |r| r.get(0)).optional()?;
                if let Some(e) = existing {
                    let prev: Amf = serde_json::from_str(&e)?;
                    if prev.from.agent != envelope.from.agent {
                        warn!("node: message id conflict for {} (different sender); refusing", envelope.id);
                        self.queue_receipt(&envelope.id, &recipient, DeliveryState::Expired)?;
                        return self.pump();
                    }
                }
                debug!("node: deliver {} to {recipient} (pair_seq {pair_seq})", envelope.id);
                let fresh = self.store_inbox(&recipient, &envelope, true)?;
                if fresh {
                    self.notify(&recipient, &envelope)?;
                }
                self.pump()?;
            }
            HubToNode::JournalAck { msg_id } => {
                self.outbox_done("kind='journal' AND msg_id=?1", &[&msg_id])?;
                self.pump()?;
            }
            HubToNode::ReceiptAck { msg_id, recipient, state } => {
                let r = rank(ds(state)) as i64;
                self.outbox_done(
                    "kind='receipt' AND msg_id=?1 AND json_extract(body,'$.recipient')=?2 AND
                     (CASE json_extract(body,'$.state') WHEN 'stored' THEN 1 WHEN 'surfaced' THEN 2 WHEN 'acked' THEN 3 WHEN 'expired' THEN 4 ELSE 0 END) <= ?3",
                    &[&msg_id, &recipient, &r],
                )?;
                self.pump()?;
            }
            HubToNode::Error { code, msg } => {
                warn!("node: hub error {code}: {msg}");
                self.push_all("hub", json!({"status": "error", "code": code, "msg": msg}));
            }
            HubToNode::Welcome { .. } | HubToNode::Enrolled { .. } | HubToNode::Ping { .. } | HubToNode::Pong { .. } => {}
        }
        Ok(())
    }

    fn queue_receipt(&self, msg_id: &str, recipient: &str, state: DeliveryState) -> Result<()> {
        self.db.execute(
            "INSERT INTO outbox(kind, msg_id, body) VALUES('receipt', ?1, ?2)",
            params![msg_id, json!({"recipient": recipient, "state": state}).to_string()],
        )?;
        Ok(())
    }

    fn finish_pending(&mut self, msg_id: &str, remote: RemoteResult) {
        if let Some(mut p) = self.pending.remove(msg_id) {
            p.result.remote = remote;
            let v = serde_json::to_value(&p.result).unwrap_or(Value::Null);
            self.reply(p.conn, ok(p.req, v));
        }
    }

    fn tick(&mut self) {
        let now_i = Instant::now();
        let (expired, keep): (Vec<Waiter>, Vec<Waiter>) = std::mem::take(&mut self.waiters).into_iter().partition(|w| w.deadline <= now_i);
        self.waiters = keep;
        for w in expired {
            self.reply(w.conn, ok(w.req, json!({"message": null, "timedOut": true})));
        }
        let late: Vec<String> = self.pending.iter().filter(|(_, p)| p.deadline <= now_i).map(|(k, _)| k.clone()).collect();
        for m in late {
            self.finish_pending(&m, RemoteResult::Queued);
        }
    }

    // ------------------------------------------------------------------ sessions

    fn cancel_waiters(&mut self, conn: Option<u64>, agent: &str) {
        let (gone, keep): (Vec<Waiter>, Vec<Waiter>) =
            std::mem::take(&mut self.waiters).into_iter().partition(|w| w.agent == agent && Some(w.conn) == conn);
        self.waiters = keep;
        for w in gone {
            self.reply(w.conn, fail(w.req, "SESSION_ENDED", "this agent session ended"));
        }
    }

    fn end_session(&mut self, agent_id: &str, reason: EndReason) {
        let Some(s) = self.sessions.remove(agent_id) else { return };
        if let Some(c) = self.conns.get_mut(&s.conn) {
            c.attached = None;
        }
        self.cancel_waiters(Some(s.conn), agent_id);
        if let Some(p) = self.profiles.iter().find(|p| p.agent_id == agent_id).cloned() {
            let rec = self.local_record(&p);
            self.push_local_roster("left", &rec, Some(s.conn));
        }
        match s.fence {
            Some(fence) => {
                self.send_live(NodeToHub::AgentDown { agent_id: agent_id.into(), session_id: s.session_id, fence, reason });
            }
            // AgentUp/Resync in flight: close it when the fence arrives.
            None if self.hub != HubStatus::Offline => {
                self.ended.insert(s.session_id, (agent_id.to_string(), reason));
            }
            None => {} // offline: the next resync omits it, so the hub marks it left
        }
        info!("node: session for {agent_id} ended ({reason:?})");
    }

    fn push_local_roster(&self, kind: &str, rec: &AgentRecord, except: Option<u64>) {
        for (id, c) in &self.conns {
            if Some(*id) == except {
                continue;
            }
            if c.watch.is_some() || c.attached.is_some() {
                self.push(*id, "roster", json!({"kind": kind, "agent": rec, "sameMachine": true, "local": true}));
            }
        }
    }

    // ------------------------------------------------------------------ inbox

    /// Store a message in a local agent's inbox (and, for remote mail, a retained
    /// `stored` receipt) in one transaction. Returns false if it was already there.
    fn store_inbox(&self, agent: &str, env: &Amf, remote: bool) -> Result<bool> {
        let tx = self.db.unchecked_transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO messages(msg_id, envelope, created_at) VALUES(?1,?2,?3)",
            params![env.id, serde_json::to_string(env)?, env.created_at.to_rfc3339()],
        )?;
        let n = tx.execute(
            "INSERT OR IGNORE INTO inbox(agent_id, msg_id, state, remote, received_at) VALUES(?1,?2,'stored',?3,?4)",
            params![agent, env.id, remote as i64, now().to_rfc3339()],
        )?;
        if remote {
            // Always (re)confirm storage, even for a duplicate delivery whose earlier receipt was lost.
            tx.execute(
                "INSERT INTO outbox(kind, msg_id, body) VALUES('receipt', ?1, ?2)",
                params![env.id, json!({"recipient": agent, "state": "stored"}).to_string()],
            )?;
        }
        tx.commit()?;
        Ok(n > 0)
    }

    fn unread_count(&self, agent: &str) -> Result<i64> {
        Ok(self.db.query_row("SELECT COUNT(*) FROM inbox WHERE agent_id=?1 AND state='stored'", params![agent], |r| r.get(0))?)
    }

    /// Advance an inbox row's state; remote mail gets a retained receipt in the same transaction.
    fn mark(&mut self, agent: &str, msg_id: &str, state: DeliveryState) -> Result<()> {
        let row: Option<(String, i64)> = self
            .db
            .query_row("SELECT state, remote FROM inbox WHERE agent_id=?1 AND msg_id=?2", params![agent, msg_id], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?;
        let Some((cur, remote)) = row else { return Ok(()) };
        if rank(ds(state)) <= rank(&cur) {
            return Ok(());
        }
        let tx = self.db.unchecked_transaction()?;
        tx.execute("UPDATE inbox SET state=?1 WHERE agent_id=?2 AND msg_id=?3", params![ds(state), agent, msg_id])?;
        if remote != 0 {
            tx.execute(
                "INSERT INTO outbox(kind, msg_id, body) VALUES('receipt', ?1, ?2)",
                params![msg_id, json!({"recipient": agent, "state": state}).to_string()],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// New mail for a local agent: satisfy one waiter, then push to watchers.
    fn notify(&mut self, agent: &str, env: &Amf) -> Result<()> {
        let reply_to = env.in_reply_to.as_deref();
        let pick = self
            .waiters
            .iter()
            .position(|w| w.agent == agent && w.in_reply_to.is_some() && w.in_reply_to.as_deref() == reply_to)
            .or_else(|| self.waiters.iter().position(|w| w.agent == agent && w.in_reply_to.is_none()));
        if let Some(i) = pick {
            let w = self.waiters.remove(i);
            self.mark(agent, &env.id, DeliveryState::Surfaced)?;
            self.reply(w.conn, ok(w.req, json!({"message": env, "unread": self.unread_count(agent)?})));
        }
        let handle = self.profiles.iter().find(|p| p.agent_id == agent).map(|p| self.handle_of(p)).unwrap_or_default();
        for (id, c) in &self.conns {
            let wants = c.attached.as_deref() == Some(agent) || matches!(&c.watch, Some(None)) || matches!(&c.watch, Some(Some(a)) if a == agent);
            if wants {
                self.push(*id, "message", json!({"recipient": agent, "recipientHandle": handle, "message": env}));
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------ IPC ops

    fn ipc(&mut self, conn: u64, req: IpcRequest) -> R<Option<Value>> {
        match req.op {
            IpcOp::Attach { agent, runner, pid, takeover } => {
                if self.conns.get(&conn).is_some_and(|c| c.attached.is_some()) {
                    return Err(IpcErr("ALREADY_ATTACHED", "this connection is already attached".into()));
                }
                let p = self.agent_for(conn, Some(&agent))?;
                if let Some(s) = self.sessions.get(&p.agent_id) {
                    if !takeover {
                        let age = (now() - s.since).num_seconds();
                        return Err(IpcErr("AGENT_BUSY", format!("{} already has a live session ({}s old); use takeover to replace it", self.handle_of(&p), age)));
                    }
                    let old = s.conn;
                    self.end_session(&p.agent_id, EndReason::Takeover);
                    self.push(old, "session", json!({"status": "ended", "reason": "takeover"}));
                }
                let sid = agentlan_proto::ids::session_id();
                self.sessions.insert(p.agent_id.clone(), Session { session_id: sid.clone(), conn, since: now(), fence: None });
                if let Some(c) = self.conns.get_mut(&conn) {
                    c.attached = Some(p.agent_id.clone());
                }
                let rec = self.local_record(&p);
                self.push_local_roster("joined", &rec, Some(conn));
                if self.hub == HubStatus::Ready {
                    self.send_live(NodeToHub::AgentUp { session_id: sid.clone(), profile: p.clone() });
                }
                info!("node: {} attached (runner {:?}, pid {:?})", self.handle_of(&p), runner, pid);
                Ok(Some(json!({
                    "agent": rec, "sessionId": sid, "provisional": true,
                    "machineId": self.cfg.machine_id, "alias": self.cfg.alias, "hub": self.hub_label(),
                    "roster": self.roster(None, None),
                    "unread": self.unread_count(&p.agent_id)?,
                })))
            }
            IpcOp::Detach => {
                let a = self.conns.get(&conn).and_then(|c| c.attached.clone()).ok_or(IpcErr("NOT_ATTACHED", "not attached".into()))?;
                self.end_session(&a, EndReason::Left);
                Ok(Some(json!({"detached": a})))
            }
            IpcOp::Whoami => {
                let p = self.agent_for(conn, None)?;
                let s = self.sessions.get(&p.agent_id);
                // Unread summary without surfacing anything (for adapters' footers).
                let mut st = self.db.prepare(
                    "SELECT m.envelope FROM inbox i JOIN messages m USING(msg_id) WHERE i.agent_id=?1 AND i.state='stored' ORDER BY i.rowid LIMIT 3",
                )?;
                let latest: Vec<Value> = st
                    .query_map(params![p.agent_id], |r| r.get::<_, String>(0))?
                    .filter_map(|e| e.ok())
                    .filter_map(|e| serde_json::from_str::<Amf>(&e).ok())
                    .map(|m| json!({"id": m.id, "from": m.from.handle, "intent": m.intent, "sameMachine": self.profiles.iter().any(|x| x.agent_id == m.from.agent)}))
                    .collect();
                drop(st);
                Ok(Some(json!({
                    "agent": self.local_record(&p), "provisional": s.is_none_or(|s| s.fence.is_none()),
                    "machineId": self.cfg.machine_id, "alias": self.cfg.alias, "hub": self.hub_label(),
                    "unread": self.unread_count(&p.agent_id)?, "unreadLatest": latest,
                })))
            }
            IpcOp::Status => {
                let outbox: i64 = self.db.query_row("SELECT COUNT(*) FROM outbox", [], |r| r.get(0))?;
                let sessions: Vec<Value> = self.sessions.iter().map(|(a, s)| json!({"agentId": a, "sessionId": s.session_id, "fenced": s.fence.is_some()})).collect();
                Ok(Some(json!({
                    "machineId": self.cfg.machine_id, "alias": self.cfg.alias, "net": self.cfg.net,
                    "hub": self.hub_label(), "seq": self.base_seq, "outbox": outbox,
                    "profiles": self.profiles.len(), "sessions": sessions,
                })))
            }
            IpcOp::ListAgents { project, role } => {
                self.reload_profiles();
                Ok(Some(json!({"agents": self.roster(project.as_deref(), role.as_deref()), "hub": self.hub_label()})))
            }
            IpcOp::ListMachines => {
                let mut ms: Vec<MachineRecord> = self.base_machines.values().cloned().collect();
                for m in ms.iter_mut() {
                    m.agents_online = if m.machine_id == self.cfg.machine_id {
                        self.sessions.len() as u32
                    } else {
                        self.base_agents.values().filter(|a| a.machine_id == m.machine_id && a.state == PresenceState::Online).count() as u32
                    };
                }
                ms.sort_by(|a, b| a.alias.cmp(&b.alias));
                Ok(Some(json!({"machines": ms, "self": {"machineId": self.cfg.machine_id, "alias": self.cfg.alias, "reported": self.reported}, "hub": self.hub_label()})))
            }
            IpcOp::Send { as_agent, draft } => self.send(conn, req.id, as_agent.as_deref(), draft),
            IpcOp::Inbox { agent, limit, message_id } => {
                let p = self.agent_for(conn, agent.as_deref())?;
                let limit = limit.unwrap_or(50).min(500) as i64;
                let rows: Vec<(String, String)> = {
                    let (sql, arg): (&str, String) = match &message_id {
                        Some(m) => ("SELECT i.msg_id, m.envelope FROM inbox i JOIN messages m USING(msg_id) WHERE i.agent_id=?1 AND i.msg_id=?2 LIMIT ?3", m.clone()),
                        None => ("SELECT i.msg_id, m.envelope FROM inbox i JOIN messages m USING(msg_id) WHERE i.agent_id=?1 AND i.state!='acked' AND ?2=?2 ORDER BY i.rowid LIMIT ?3", String::new()),
                    };
                    let mut st = self.db.prepare(sql)?;
                    st.query_map(params![p.agent_id, arg, limit], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<Result<_, _>>()?
                };
                let mut msgs = Vec::new();
                for (mid, env) in rows {
                    self.mark(&p.agent_id, &mid, DeliveryState::Surfaced)?;
                    msgs.push(serde_json::from_str::<Value>(&env)?);
                }
                self.pump()?;
                Ok(Some(json!({"agent": self.handle_of(&p), "messages": msgs, "unread": self.unread_count(&p.agent_id)?})))
            }
            IpcOp::Ack { agent, message_id } => {
                let p = self.agent_for(conn, agent.as_deref())?;
                let exists: Option<i64> =
                    self.db.query_row("SELECT 1 FROM inbox WHERE agent_id=?1 AND msg_id=?2", params![p.agent_id, message_id], |r| r.get(0)).optional()?;
                if exists.is_none() {
                    return Err(IpcErr("UNKNOWN_MESSAGE", format!("{message_id} is not in {}'s inbox", self.handle_of(&p))));
                }
                self.mark(&p.agent_id, &message_id, DeliveryState::Acked)?;
                self.pump()?;
                Ok(Some(json!({"acked": message_id})))
            }
            IpcOp::Wait { agent, timeout_ms, in_reply_to } => {
                let p = self.agent_for(conn, agent.as_deref())?;
                // A reply-waiter takes the reply even if it was already seen; a plain wait
                // takes only unseen mail and never a reply someone is waiting for.
                let next: Option<(String, String)> = match &in_reply_to {
                    Some(rt) => self
                        .db
                        .query_row(
                            "SELECT i.msg_id, m.envelope FROM inbox i JOIN messages m USING(msg_id)
                             WHERE i.agent_id=?1 AND i.state!='acked' AND json_extract(m.envelope,'$.inReplyTo')=?2 ORDER BY i.rowid LIMIT 1",
                            params![p.agent_id, rt],
                            |r| Ok((r.get(0)?, r.get(1)?)),
                        )
                        .optional()?,
                    None => {
                        let pending: Vec<String> = self.waiters.iter().filter_map(|w| w.in_reply_to.clone()).collect();
                        let mut st = self.db.prepare(
                            "SELECT i.msg_id, m.envelope, json_extract(m.envelope,'$.inReplyTo') FROM inbox i JOIN messages m USING(msg_id)
                             WHERE i.agent_id=?1 AND i.state='stored' ORDER BY i.rowid",
                        )?;
                        let rows: Vec<(String, String, Option<String>)> =
                            st.query_map(params![p.agent_id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?.collect::<Result<_, _>>()?;
                        drop(st);
                        rows.into_iter().find(|(_, _, rt)| rt.as_ref().is_none_or(|x| !pending.contains(x))).map(|(m, e, _)| (m, e))
                    }
                };
                if let Some((mid, env)) = next {
                    self.mark(&p.agent_id, &mid, DeliveryState::Surfaced)?;
                    self.pump()?;
                    return Ok(Some(json!({"message": serde_json::from_str::<Value>(&env)?, "unread": self.unread_count(&p.agent_id)?})));
                }
                let t = timeout_ms.min(300_000);
                self.waiters.push(Waiter { conn, req: req.id, agent: p.agent_id, deadline: Instant::now() + Duration::from_millis(t), in_reply_to });
                Ok(None)
            }
            IpcOp::Peek { agent } => {
                let p = self.agent_for(conn, Some(&agent))?;
                let mut st = self.db.prepare(
                    "SELECT m.envelope FROM inbox i JOIN messages m USING(msg_id) WHERE i.agent_id=?1 AND i.state='stored' ORDER BY i.rowid LIMIT 5",
                )?;
                let latest: Vec<Value> = st
                    .query_map(params![p.agent_id], |r| r.get::<_, String>(0))?
                    .filter_map(|e| e.ok())
                    .filter_map(|e| serde_json::from_str::<Amf>(&e).ok())
                    .map(|m| json!({"id": m.id, "from": m.from.handle, "intent": m.intent, "sameMachine": self.profiles.iter().any(|x| x.agent_id == m.from.agent)}))
                    .collect();
                drop(st);
                let project = p.project.clone();
                let mut roster = self.roster(None, None);
                roster.retain(|e| e.agent.profile.agent_id != p.agent_id);
                roster.sort_by_key(|e| (e.agent.profile.project != project, !e.same_machine, e.agent.state != PresenceState::Online));
                Ok(Some(json!({
                    "agent": self.local_record(&p), "alias": self.cfg.alias, "machineId": self.cfg.machine_id, "hub": self.hub_label(),
                    "unread": self.unread_count(&p.agent_id)?, "unreadLatest": latest, "roster": roster,
                })))
            }
            IpcOp::CancelWait => {
                let (gone, keep): (Vec<Waiter>, Vec<Waiter>) = std::mem::take(&mut self.waiters).into_iter().partition(|w| w.conn == conn);
                self.waiters = keep;
                for w in &gone {
                    self.reply(w.conn, fail(w.req, "CANCELLED", "wait cancelled"));
                }
                Ok(Some(json!({"cancelled": gone.len()})))
            }
            IpcOp::Unsurface { agent, message_id } => {
                let p = self.agent_for(conn, agent.as_deref())?;
                let n = self.db.execute(
                    "UPDATE inbox SET state='stored' WHERE agent_id=?1 AND msg_id=?2 AND state='surfaced'",
                    params![p.agent_id, message_id],
                )?;
                Ok(Some(json!({"unsurfaced": n})))
            }
            IpcOp::Watch { agent } => {
                let filter = match agent {
                    Some(a) => Some(self.agent_for(conn, Some(&a))?.agent_id),
                    None => None,
                };
                if let Some(c) = self.conns.get_mut(&conn) {
                    c.watch = Some(filter);
                }
                Ok(Some(json!({"watching": true, "hub": self.hub_label()})))
            }
        }
    }

    fn send(&mut self, conn: u64, req_id: u64, as_agent: Option<&str>, mut draft: SendDraft) -> R<Option<Value>> {
        let sender = self.agent_for(conn, as_agent)?;
        // Caller retry keys are scoped to the sender.
        let client_id = draft.id.take();
        if let Some(cid) = &client_id {
            let canon = canonical_msg_id(&sender.agent_id, cid);
            let prev: Option<String> = self.db.query_row("SELECT result FROM sent WHERE msg_id=?1", params![canon], |r| r.get(0)).optional()?;
            if let Some(p) = prev {
                let mut res: SendResult = serde_json::from_str(&p)?;
                res.duplicate = true;
                return Ok(Some(serde_json::to_value(res)?));
            }
            draft.id = Some(canon);
        }
        let from = From { agent: sender.agent_id.clone(), handle: Some(self.handle_of(&sender)), role: Some(sender.role.clone()), origin: Origin::Agent };
        let env = draft.into_amf(from, DEFAULT_TTL_HOURS).map_err(|e| IpcErr("INVALID", e.to_string()))?;
        if serde_json::to_vec(&env)?.len() + FRAME_HEADROOM > MAX_FRAME {
            return Err(IpcErr("TOO_LARGE", "message exceeds the frame limit".into()));
        }

        // Resolve: the node is authoritative for its own agents.
        let attached_match = |pr: &AgentProfile, f: &dyn Fn(&AgentProfile) -> bool| pr.agent_id != sender.agent_id && self.sessions.contains_key(&pr.agent_id) && f(pr);
        let (mut local, mut needs_remote): (Vec<AgentProfile>, bool) = match &env.to {
            Target::Agent { agent } => match self.profiles.iter().find(|p| &p.agent_id == agent) {
                Some(p) => (vec![p.clone()], false),
                None => (vec![], true),
            },
            Target::Handle { handle } => match agentlan_proto::ids::split_handle(handle) {
                Some((n, a)) if a == self.cfg.alias => match self.profiles.iter().find(|p| p.name == n) {
                    Some(p) => (vec![p.clone()], false),
                    None => return Err(IpcErr("UNKNOWN_RECIPIENT", format!("no agent '{n}' on this machine"))),
                },
                Some(_) => (vec![], true),
                None => return Err(IpcErr("INVALID", format!("bad handle '{handle}'"))),
            },
            Target::Role { role, project, .. } => (
                self.profiles.iter().filter(|p| attached_match(p, &|p| &p.role == role && project.as_ref().is_none_or(|pr| p.project.as_deref() == Some(pr.as_str())))).cloned().collect(),
                true,
            ),
            Target::Project { project, .. } => {
                (self.profiles.iter().filter(|p| attached_match(p, &|p| p.project.as_deref() == Some(project.as_str()))).cloned().collect(), true)
            }
            Target::Machine { machine } => {
                if machine == &self.cfg.machine_id || machine == &self.cfg.alias {
                    (self.profiles.iter().filter(|p| attached_match(p, &|_| true)).cloned().collect(), false)
                } else {
                    (vec![], true)
                }
            }
            Target::All => (self.profiles.iter().filter(|p| attached_match(p, &|_| true)).cloned().collect(), true),
        };
        if matches!(&env.to, Target::Role { pick: Pick::One, .. } | Target::Project { pick: Pick::One, .. }) && !local.is_empty() {
            local.truncate(1);
            needs_remote = false; // a committed local pick skips the remote leg
        }
        let local_ids: Vec<String> = local.iter().map(|p| p.agent_id.clone()).collect();
        let result = SendResult {
            id: env.id.clone(),
            thread_id: env.thread_id.clone(),
            local: local.len() as u32,
            remote: if needs_remote { RemoteResult::Queued } else { RemoteResult::None },
            duplicate: false,
            client_id,
        };

        // One transaction: message + local inbox rows + dedup record + deferred work.
        {
            let tx = self.db.unchecked_transaction()?;
            tx.execute(
                "INSERT INTO messages(msg_id, envelope, created_at) VALUES(?1,?2,?3)",
                params![env.id, serde_json::to_string(&env)?, env.created_at.to_rfc3339()],
            )
            .map_err(|e| match &e {
                agentlan_store::rusqlite::Error::SqliteFailure(f, _) if f.code == agentlan_store::rusqlite::ErrorCode::ConstraintViolation => {
                    IpcErr("ID_CONFLICT", format!("message id {} already exists", env.id))
                }
                _ => IpcErr("STORAGE", e.to_string()),
            })?;
            for id in &local_ids {
                tx.execute(
                    "INSERT OR IGNORE INTO inbox(agent_id, msg_id, state, remote, received_at) VALUES(?1,?2,'stored',0,?3)",
                    params![id, env.id, now().to_rfc3339()],
                )?;
            }
            tx.execute("INSERT INTO sent(msg_id, sender, result) VALUES(?1,?2,?3)", params![env.id, sender.agent_id, serde_json::to_string(&result)?])?;
            if needs_remote {
                tx.execute(
                    "INSERT INTO outbox(kind, msg_id, body) VALUES('submit', ?1, ?2)",
                    params![env.id, json!({"agentId": sender.agent_id, "envelope": env, "localRecipients": local_ids}).to_string()],
                )?;
            }
            if !local_ids.is_empty() {
                let entry = JournalEntry {
                    msg_id: env.id.clone(),
                    thread_id: env.thread_id.clone(),
                    from: sender.agent_id.clone(),
                    recipients: local_ids.clone(),
                    intent: env.intent.clone(),
                    bytes: serde_json::to_vec(&env.parts).map(|v| v.len() as u64).unwrap_or(0),
                    created_at: env.created_at,
                };
                tx.execute("INSERT INTO outbox(kind, msg_id, body) VALUES('journal', ?1, ?2)", params![env.id, serde_json::to_string(&entry)?])?;
            }
            tx.commit()?;
        }

        // Local leg: deliver now, over IPC only.
        for id in &local_ids {
            self.notify(id, &env)?;
        }
        self.pump()?;
        if needs_remote && self.hub == HubStatus::Ready {
            // Answer when the hub decides (or after SEND_WAIT as "queued").
            self.pending.insert(env.id.clone(), PendingSend { conn, req: req_id, result, deadline: Instant::now() + SEND_WAIT });
            return Ok(None);
        }
        Ok(Some(serde_json::to_value(result)?))
    }
}

#[cfg(test)]
mod tests {
    use super::canonical_msg_id;

    #[test]
    fn retry_keys_are_sender_scoped() {
        let a = canonical_msg_id("agt_A", "build-42");
        assert_eq!(a, canonical_msg_id("agt_A", "build-42"));
        assert_ne!(a, canonical_msg_id("agt_B", "build-42"));
        assert!(a.starts_with("msg_") && a.len() == 30);
    }
}
