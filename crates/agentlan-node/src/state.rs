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
use agentlan_proto::records::{AgentMode, AgentProfile, AgentRecord, EndReason, MachineRecord, MachineReported, PresenceState, Readiness, RosterEntry};
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
CREATE TABLE IF NOT EXISTS budget(key TEXT NOT NULL, window INTEGER NOT NULL, count INTEGER NOT NULL, PRIMARY KEY(key, window));
CREATE TABLE IF NOT EXISTS paused(key TEXT PRIMARY KEY, paused_at TEXT NOT NULL, reason TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS request_recipients(msg_id TEXT NOT NULL, agent_id TEXT NOT NULL, PRIMARY KEY(msg_id, agent_id));
"#;

/// Loop-guard limits (cost/safety brake; agents run as the same OS user, so this is
/// not a security boundary). Overridable via env for tests and tuning.
struct Limits {
    per_agent_hour: i64,
    per_pair_hour: i64,
    per_thread: i64,
}

impl Limits {
    fn from_env() -> Limits {
        let get = |k: &str, d: i64| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
        Limits {
            per_agent_hour: get("AGENTLAN_LIMIT_AGENT_HOUR", 120),
            per_pair_hour: get("AGENTLAN_LIMIT_PAIR_HOUR", 40),
            per_thread: get("AGENTLAN_LIMIT_THREAD", 30),
        }
    }
}

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
    /// A wake (see `wake.rs`) finished.
    WakeDone { agent_id: String, result: crate::wake::WakeResult },
}

/// Worker wake limits (env-overridable).
struct WakeLimits {
    per_hour: i64,
    concurrent: usize,
    debounce: Duration,
}

impl WakeLimits {
    fn from_env() -> WakeLimits {
        let get = |k: &str, d: u64| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
        WakeLimits {
            per_hour: get("AGENTLAN_WAKE_PER_HOUR", 12) as i64,
            concurrent: get("AGENTLAN_WAKE_CONCURRENT", 2) as usize,
            debounce: Duration::from_millis(get("AGENTLAN_WAKE_DEBOUNCE_MS", 1500)),
        }
    }
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
    /// Process hosting the adapter (e.g. a Claude Code session); hooks look identities up by it.
    host_pid: Option<u32>,
    /// The adapter (or other client) process that holds this session.
    client_pid: Option<u32>,
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
    /// Set for an atomic Request: the send result to report alongside the reply.
    request: Option<SendResult>,
}

/// A send committed to the node (or an earlier identical send, for retries).
struct Committed {
    env: Amf,
    result: SendResult,
    needs_remote: bool,
    local_ids: Vec<String>,
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
    /// Profile JSON last registered with the hub, by agent id (to upsert new/edited ones).
    registered: HashMap<String, String>,
    /// profiles.json modification time, polled on ticks so `agent add` takes effect live.
    profiles_mtime: Option<std::time::SystemTime>,
    ticks: u64,
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
    /// Set by a Shutdown request: the state loop exits after replying.
    stopping: bool,
    /// Worker wake: due time per agent (debounced), in-flight wakes, and agents that got
    /// new mail while their wake was running.
    wake_due: HashMap<String, Instant>,
    wake_inflight: HashMap<String, (Instant, tokio::task::AbortHandle)>,
    wake_again: HashSet<String>,
    /// When each worker's last wake finished (its MCP session may close a moment later).
    wake_finished: HashMap<String, Instant>,
    cmd_tx: mpsc::UnboundedSender<NCmd>,
    rt: tokio::runtime::Handle,
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
    pub fn new(
        home: PathBuf,
        db: Connection,
        cfg: NodeFile,
        reported: MachineReported,
        link: mpsc::UnboundedSender<LinkCtl>,
        cmd_tx: mpsc::UnboundedSender<NCmd>,
        rt: tokio::runtime::Handle,
    ) -> Result<NodeState> {
        agentlan_store::migrate(&db, SCHEMA)?;
        let profiles = crate::load_profiles(&home)?.agents;
        Ok(NodeState {
            home,
            db,
            cfg,
            reported,
            profiles,
            registered: HashMap::new(),
            profiles_mtime: None,
            ticks: 0,
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
            stopping: false,
            wake_due: HashMap::new(),
            wake_inflight: HashMap::new(),
            wake_again: HashSet::new(),
            wake_finished: HashMap::new(),
            cmd_tx,
            rt,
        })
    }

    pub fn run(mut self, mut rx: mpsc::UnboundedReceiver<NCmd>, stop: tokio::sync::oneshot::Sender<()>) {
        while let Some(c) = rx.blocking_recv() {
            if let Err(e) = self.handle(c) {
                warn!("node: command failed: {e:#}");
            }
            if self.stopping {
                info!("node: stopping (requested over IPC)");
                // Stop running wakes (dropping a wake kills its agent process).
                for (_, (_, h)) in self.wake_inflight.drain() {
                    h.abort();
                }
                // Every write is its own transaction, so nothing is half-done here; give the
                // IPC writer a moment to deliver the reply, then end the node's tasks.
                std::thread::sleep(Duration::from_millis(200));
                let _ = stop.send(());
                return;
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
            NCmd::WakeDone { agent_id, result } => self.wake_done(&agent_id, result)?,
            NCmd::Tick => {
                self.ticks += 1;
                if self.ticks % 10 == 0 {
                    let m = std::fs::metadata(self.home.join("node").join("profiles.json")).and_then(|m| m.modified()).ok();
                    // Only remember the timestamp once the file parsed, so a read that
                    // caught a partial write is retried on the next poll.
                    if m != self.profiles_mtime && self.reload_profiles() {
                        self.profiles_mtime = m;
                    }
                }
                self.tick();
                self.fire_wakes()?;
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

    /// Re-read profiles.json; returns false if it could not be parsed (e.g. mid-write).
    fn reload_profiles(&mut self) -> bool {
        let ok = match crate::load_profiles(&self.home) {
            Ok(pf) => {
                self.profiles = pf.agents;
                true
            }
            Err(e) => {
                warn!("node: cannot read profiles: {e:#}");
                false
            }
        };
        self.register_changed_profiles();
        ok
    }

    /// Agents added or edited while the node runs: register them with the hub before
    /// anything they send (same link, so the upsert is processed first).
    fn register_changed_profiles(&mut self) {
        if self.hub == HubStatus::Ready {
            let changed: Vec<AgentProfile> = self
                .profiles
                .iter()
                .filter(|p| self.registered.get(&p.agent_id).is_none_or(|j| serde_json::to_string(p).ok().as_ref() != Some(j)))
                .cloned()
                .collect();
            for p in changed {
                if self.send_live(NodeToHub::ProfileUpsert { profile: p.clone() }) {
                    self.registered.insert(p.agent_id.clone(), serde_json::to_string(&p).unwrap_or_default());
                }
            }
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
            project.is_none_or(|p| agentlan_proto::ids::project_matches(e.agent.profile.project.as_deref(), p))
                && role.is_none_or(|r| e.agent.profile.role == r)
                && e.agent.state != PresenceState::Retired
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
                        exclude: serde_json::from_value(v["exclude"].clone()).unwrap_or_default(),
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

    /// A request's send result as it stands now (the hub's verdict may have arrived since
    /// the request was committed).
    fn current_sent(&self, r: &SendResult) -> SendResult {
        let cur: Option<String> = self.db.query_row("SELECT result FROM sent WHERE msg_id=?1", params![r.id], |row| row.get(0)).optional().ok().flatten();
        match cur.and_then(|c| serde_json::from_str::<SendResult>(&c).ok()) {
            Some(mut now) => {
                now.client_id = r.client_id.clone();
                now.duplicate = r.duplicate;
                now
            }
            None => r.clone(),
        }
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
        // Resync carries every profile.
        self.registered = self.profiles.iter().map(|p| (p.agent_id.clone(), serde_json::to_string(p).unwrap_or_default())).collect();
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
                // Profiles added or edited while resyncing weren't in the Resync payload.
                self.register_changed_profiles();
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
            HubToNode::Accepted { msg_id, remote, recipients } => {
                self.record_remote_recipients(&msg_id, &recipients)?;
                if remote == 0 {
                    // A request nobody can receive should not wait for its timeout.
                    let (gone, keep): (Vec<Waiter>, Vec<Waiter>) =
                        std::mem::take(&mut self.waiters).into_iter().partition(|w| w.request.is_some() && w.in_reply_to.as_deref() == Some(msg_id.as_str()));
                    self.waiters = keep;
                    for w in gone {
                        self.reply(w.conn, fail(w.req, "NOT_DELIVERED", "no agent matched the request (role/project targets need an online agent)"));
                    }
                }
                self.settle_submit(&msg_id, RemoteResult::Accepted { count: remote })?
            }
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
            let v = match &w.request {
                Some(r) => json!({"status": "pending", "sent": self.current_sent(r), "reply": null}),
                None => json!({"message": null, "timedOut": true}),
            };
            self.reply(w.conn, ok(w.req, v));
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
        let Some(s) = self.sessions.remove(agent_id) else {
            return;
        };
        if let Err(e) = self.wake_followup(agent_id) {
            warn!("wake: follow-up check failed: {e:#}");
        }
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
        let Some((cur, remote)) = row else {
            return Ok(());
        };
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
    /// Is `sender` one of the recipients actually routed for request `request_id`?
    fn allowed_replier(&self, request_id: &str, sender: &str) -> Result<bool> {
        Ok(self
            .db
            .query_row("SELECT 1 FROM request_recipients WHERE msg_id=?1 AND agent_id=?2", params![request_id, sender], |_| Ok(()))
            .optional()?
            .is_some())
    }

    /// Give a message to one matching waiter: a request waiter for a reply from its real
    /// recipient first, otherwise a plain waiter. Returns true if consumed.
    fn offer_to_waiters(&mut self, agent: &str, env: &Amf) -> Result<bool> {
        let mut pick = None;
        if let Some(rt) = env.in_reply_to.as_deref() {
            if self.allowed_replier(rt, &env.from.agent)? {
                pick = self.waiters.iter().position(|w| w.agent == agent && w.in_reply_to.as_deref() == Some(rt));
            }
        }
        let pending: Vec<String> = self.waiters.iter().filter_map(|w| w.in_reply_to.clone()).collect();
        let reserved = env.in_reply_to.as_ref().is_some_and(|rt| pending.contains(rt)) && pick.is_none() && {
            // A reply someone is waiting for is never handed to a plain waiter.
            env.in_reply_to.as_deref().map(|rt| self.allowed_replier(rt, &env.from.agent)).transpose()?.unwrap_or(false)
        };
        if pick.is_none() && !reserved {
            pick = self.waiters.iter().position(|w| w.agent == agent && w.in_reply_to.is_none());
        }
        let Some(i) = pick else { return Ok(false) };
        let w = self.waiters.remove(i);
        self.mark(agent, &env.id, DeliveryState::Surfaced)?;
        let v = match &w.request {
            Some(r) => {
                json!({"status": "replied", "sent": self.current_sent(r), "reply": env, "unread": self.unread_count(agent)?})
            }
            None => json!({"message": env, "unread": self.unread_count(agent)?}),
        };
        self.reply(w.conn, ok(w.req, v));
        Ok(true)
    }

    fn notify(&mut self, agent: &str, env: &Amf) -> Result<()> {
        self.maybe_wake(agent, env)?;
        // `surfaced`: a waiting call (wait_for_message / request) already took it, so the
        // agent has it in hand and live delivery needn't point at it.
        let surfaced = self.offer_to_waiters(agent, env)?;
        let handle = self.profiles.iter().find(|p| p.agent_id == agent).map(|p| self.handle_of(p)).unwrap_or_default();
        for (id, c) in &self.conns {
            let wants = c.attached.as_deref() == Some(agent) || matches!(&c.watch, Some(None)) || matches!(&c.watch, Some(Some(a)) if a == agent);
            if wants {
                self.push(*id, "message", json!({"recipient": agent, "recipientHandle": handle, "message": env, "surfaced": surfaced}));
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------ IPC ops

    fn ipc(&mut self, conn: u64, req: IpcRequest) -> R<Option<Value>> {
        match req.op {
            IpcOp::Attach { agent, runner, pid, takeover, host_pid } => {
                if self.conns.get(&conn).is_some_and(|c| c.attached.is_some()) {
                    return Err(IpcErr("ALREADY_ATTACHED", "this connection is already attached".into()));
                }
                let p = self.agent_for(conn, Some(&agent))?;
                self.attach_session(conn, p, runner, pid, host_pid, takeover).map(Some)
            }
            IpcOp::Register { name, role, project, description, runner, pid, host_pid } => {
                self.register(conn, name, role, project, description, runner, pid, host_pid).map(Some)
            }
            IpcOp::WakeNow { agent } => {
                let p = self.agent_for(conn, Some(&agent))?;
                if self.wake_enabled(&p.agent_id).is_none() {
                    return Err(IpcErr("NOT_WAKEABLE", format!("{} has no wake configured (`agentlan wake enable`)", self.handle_of(&p))));
                }
                if self.unread_count(&p.agent_id)? == 0 {
                    return Err(IpcErr("NO_MAIL", format!("{} has no unread messages", self.handle_of(&p))));
                }
                if self.sessions.contains_key(&p.agent_id) && !self.wake_inflight.contains_key(&p.agent_id) {
                    return Err(IpcErr("AGENT_BUSY", format!("{} is running; it reads its own mail", self.handle_of(&p))));
                }
                self.wake_due.insert(p.agent_id.clone(), Instant::now());
                Ok(Some(json!({"scheduled": self.handle_of(&p)})))
            }
            IpcOp::WakeStatus => {
                let wf = crate::load_wake(&self.home).map_err(|e| IpcErr("WAKE_CONFIG", format!("{e:#}")))?;
                let mut out = vec![];
                for (id, c) in wf.workers {
                    let handle = self.profiles.iter().find(|p| p.agent_id == id).map(|p| self.handle_of(p)).unwrap_or_else(|| c.name.clone());
                    out.push(json!({
                        "agentId": id, "handle": handle, "config": c,
                        "running": self.wake_inflight.contains_key(&id), "due": self.wake_due.contains_key(&id),
                        "online": self.sessions.contains_key(&id), "unread": self.unread_count(&id)?,
                    }));
                }
                Ok(Some(json!({"workers": out})))
            }
            IpcOp::Shutdown => {
                self.stopping = true;
                Ok(Some(json!({"stopping": true})))
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
                let mut st = self.db.prepare("SELECT key, paused_at, reason FROM paused ORDER BY paused_at")?;
                let paused: Vec<Value> = st
                    .query_map([], |r| {
                        let key: String = r.get(0)?;
                        Ok(json!({"key": key, "threadId": key.strip_prefix("thread:"), "pausedAt": r.get::<_, String>(1)?, "reason": r.get::<_, String>(2)?}))
                    })?
                    .filter_map(|x| x.ok())
                    .collect();
                drop(st);
                Ok(Some(json!({
                    "machineId": self.cfg.machine_id, "alias": self.cfg.alias, "net": self.cfg.net,
                    "hub": self.hub_label(), "seq": self.base_seq, "outbox": outbox,
                    "profiles": self.profiles.len(), "sessions": sessions, "pausedThreads": paused,
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
                             WHERE i.agent_id=?1 AND i.state!='acked' AND json_extract(m.envelope,'$.inReplyTo')=?2
                               AND json_extract(m.envelope,'$.from.agent') IN (SELECT agent_id FROM request_recipients WHERE msg_id=?2)
                             ORDER BY i.rowid LIMIT 1",
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
                self.waiters.push(Waiter { conn, req: req.id, agent: p.agent_id, deadline: Instant::now() + Duration::from_millis(t), in_reply_to, request: None });
                Ok(None)
            }
            IpcOp::Request { as_agent, draft, timeout_ms } => self.request(conn, req.id, as_agent.as_deref(), draft, timeout_ms),
            IpcOp::Peek { agent, host_pids } => {
                let p = match agent {
                    Some(a) => self.agent_for(conn, Some(&a))?,
                    None => {
                        // The identity registered from the nearest host process. Subagents of
                        // one session share its adapter: the earliest registration is the main
                        // agent. If several adapters share that host (one process hosting several
                        // sessions), we can't tell which is ours: report nothing.
                        let mut hit = None;
                        for hp in &host_pids {
                            let m: Vec<(&String, &Session)> = self.sessions.iter().filter(|(_, s)| s.host_pid == Some(*hp)).collect();
                            if m.is_empty() {
                                continue;
                            }
                            let adapters: HashSet<Option<u32>> = m.iter().map(|(_, s)| s.client_pid).collect();
                            if adapters.len() > 1 {
                                return Err(IpcErr("AMBIGUOUS_IDENTITY", "several AgentLAN sessions share this host process".into()));
                            }
                            hit = m.iter().min_by_key(|(_, s)| s.since).map(|(a, _)| (*a).clone());
                            break;
                        }
                        let a = hit.ok_or(IpcErr("NOT_REGISTERED", "no AgentLAN identity is registered from this session".into()))?;
                        self.agent_for(conn, Some(&a))?
                    }
                };
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
            IpcOp::ResumeThread { thread_id } => {
                // Accepts a thread id (thr_…) or a paused key (thread:… / pair:…) only.
                let key = if thread_id.contains(':') { thread_id.clone() } else { format!("thread:{thread_id}") };
                if !(key.starts_with("thread:") || key.starts_with("pair:")) {
                    return Err(IpcErr("INVALID", format!("'{key}' is not a thread or pair key")));
                }
                let tx = self.db.unchecked_transaction()?;
                let n = tx.execute("DELETE FROM paused WHERE key=?1", params![key])?;
                if n == 0 {
                    return Err(IpcErr("NOT_PAUSED", format!("{key} is not paused")));
                }
                tx.execute("DELETE FROM budget WHERE key=?1", params![key])?;
                tx.commit()?;
                info!("node: {key} resumed by a human");
                Ok(Some(json!({"resumed": true, "key": key})))
            }
            IpcOp::CancelWait { req_id } => {
                let (gone, keep): (Vec<Waiter>, Vec<Waiter>) =
                    std::mem::take(&mut self.waiters).into_iter().partition(|w| w.conn == conn && req_id.is_none_or(|r| w.req == r));
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
                if n > 0 {
                    // A waiter registered meanwhile should get it now, not time out.
                    let env: Option<String> = self.db.query_row("SELECT envelope FROM messages WHERE msg_id=?1", params![message_id], |r| r.get(0)).optional()?;
                    if let Some(env) = env {
                        let env: Amf = serde_json::from_str(&env)?;
                        self.offer_to_waiters(&p.agent_id, &env)?;
                    }
                }
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

    #[allow(clippy::too_many_arguments)]
    fn attach_session(&mut self, conn: u64, p: AgentProfile, runner: Option<String>, pid: Option<u32>, host_pid: Option<u32>, takeover: bool) -> R<Value> {
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
        self.sessions.insert(p.agent_id.clone(), Session { session_id: sid.clone(), conn, since: now(), fence: None, host_pid, client_pid: pid });
        if let Some(c) = self.conns.get_mut(&conn) {
            c.attached = Some(p.agent_id.clone());
        }
        let rec = self.local_record(&p);
        self.push_local_roster("joined", &rec, Some(conn));
        if self.hub == HubStatus::Ready {
            self.send_live(NodeToHub::AgentUp { session_id: sid.clone(), profile: p.clone() });
        }
        info!("node: {} attached (runner {:?}, pid {:?})", self.handle_of(&p), runner, pid);
        Ok(json!({
            "agent": rec, "sessionId": sid, "provisional": true,
            "machineId": self.cfg.machine_id, "alias": self.cfg.alias, "hub": self.hub_label(),
            "roster": self.roster(None, None),
            "unread": self.unread_count(&p.agent_id)?,
        }))
    }

    /// Write one profile change through the locked file, then pick it up.
    fn save_profile(&mut self, f: impl FnOnce(&mut crate::ProfilesFile) -> Result<AgentProfile>) -> R<AgentProfile> {
        let p = crate::update_profiles(&self.home, f).map_err(|e| IpcErr("PROFILE_WRITE", format!("{e:#}")))?;
        self.reload_profiles();
        Ok(p)
    }

    /// An agent (or subagent) assigns itself a name and role. See `IpcOp::Register`.
    #[allow(clippy::too_many_arguments)]
    fn register(
        &mut self,
        conn: u64,
        name: Option<String>,
        role: String,
        project: Option<String>,
        description: Option<String>,
        runner: Option<String>,
        pid: Option<u32>,
        host_pid: Option<u32>,
    ) -> R<Value> {
        use agentlan_proto::ids::normalize_name;
        let role = normalize_name(&role);
        if role.is_empty() || role.len() > 32 {
            return Err(IpcErr("INVALID", "role must be 1-32 letters, digits or dashes (e.g. \"backend\", \"reviewer\")".into()));
        }
        let explicit = match name.as_deref().map(normalize_name) {
            Some(n) if n.is_empty() || n.len() > 40 => {
                return Err(IpcErr("INVALID", "name must be 1-40 letters, digits or dashes".into()));
            }
            other => other,
        };
        let clip = |v: Option<String>, n: usize| v.map(|v| v.trim().chars().take(n).collect::<String>()).filter(|v| !v.is_empty());
        let project = clip(project, 120);
        let description = clip(description, 300);
        let runner = runner.unwrap_or_else(|| "generic".into());
        self.reload_profiles();

        // Already registered on this connection: change role/project/description in place.
        if let Some(cur) = self.conns.get(&conn).and_then(|c| c.attached.clone()) {
            let me = self.find_local(&cur).ok_or(IpcErr("UNKNOWN_AGENT", "attached profile vanished".into()))?;
            if explicit.as_ref().is_some_and(|n| *n != me.name) {
                return Err(IpcErr(
                    "ALREADY_ATTACHED",
                    format!("this connection is registered as {}; open another connection for another identity", self.handle_of(&me)),
                ));
            }
            let id = me.agent_id.clone();
            let p = self.save_profile(|pf| {
                let a = pf.agents.iter_mut().find(|a| a.agent_id == id).ok_or_else(|| anyhow::anyhow!("profile {id} not found"))?;
                a.role = role;
                if project.is_some() {
                    a.project = project;
                }
                if description.is_some() {
                    a.description = description;
                }
                Ok(a.clone())
            })?;
            let rec = self.local_record(&p);
            self.push_local_roster("updated", &rec, Some(conn));
            self.register_changed_profiles();
            return Ok(json!({
                "agent": rec, "updated": true, "created": false,
                "machineId": self.cfg.machine_id, "alias": self.cfg.alias, "hub": self.hub_label(),
                "roster": self.roster(None, None), "unread": self.unread_count(&p.agent_id)?,
            }));
        }

        // Pick a name and write the profile in one locked step, so a concurrent
        // `agent add` (e.g. a worker profile with the same name) can't slip in between.
        // Requested name: reuse that offline interactive profile (keeps its recorded id).
        // No name: the role, then role-2, role-3, … reusing an offline profile only when
        // it's for the same project (never another project's identity by accident).
        let live: HashSet<String> = self.sessions.keys().cloned().collect();
        let base = explicit.clone().unwrap_or_else(|| role.clone());
        let stem: String = base.chars().take(36).collect();
        let r2 = runner.clone();
        let picked = crate::update_profiles(&self.home, |pf| {
            for i in 1..=99 {
                let cand = if i == 1 { base.clone() } else { format!("{stem}-{i}") };
                let Some(a) = pf.agents.iter_mut().find(|a| a.name == cand) else {
                    let p = AgentProfile {
                        agent_id: agentlan_proto::ids::agent_id(),
                        name: cand,
                        role,
                        project,
                        runner: r2,
                        mode: AgentMode::Interactive,
                        capabilities: vec![],
                        description,
                    };
                    pf.agents.push(p.clone());
                    return Ok(Ok((p, true)));
                };
                let busy = live.contains(&a.agent_id);
                if explicit.is_some() {
                    // Worker/script profiles are set up by the user; agents can't claim them.
                    if a.mode != AgentMode::Interactive {
                        return Ok(Err(IpcErr("RESERVED", format!("'{cand}' is a {:?} profile set up by the user on this PC; choose another name", a.mode).to_lowercase())));
                    }
                    if busy {
                        return Ok(Err(IpcErr("AGENT_BUSY", format!("'{cand}' is in use by another live session; choose another name (e.g. {cand}-2) or omit name"))));
                    }
                } else if a.mode != AgentMode::Interactive || busy || a.project != project {
                    continue;
                }
                // A returning agent keeps its recorded id.
                a.role = role;
                a.runner = r2;
                if project.is_some() {
                    a.project = project;
                }
                if description.is_some() {
                    a.description = description;
                }
                return Ok(Ok((a.clone(), false)));
            }
            Ok(Err(IpcErr("AGENT_BUSY", format!("too many agents named {stem}-…; pass a different name"))))
        });
        let (p, created) = match picked {
            Ok(Ok(x)) => x,
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(IpcErr("PROFILE_WRITE", format!("{e:#}"))),
        };
        self.reload_profiles();
        // Upsert before AgentUp (same link, so the hub learns the new role first).
        self.register_changed_profiles();
        let mut v = self.attach_session(conn, p, Some(runner), pid, host_pid, false)?;
        v["created"] = json!(created);
        v["updated"] = json!(!created);
        Ok(v)
    }

    // ------------------------------------------------------------------ worker wake

    fn wake_enabled(&self, agent: &str) -> Option<AgentProfile> {
        self.profiles.iter().find(|p| p.agent_id == agent && p.mode == AgentMode::Worker && p.capabilities.iter().any(|c| c == "wake")).cloned()
    }

    /// New mail for a local agent: if it's an idle wake-enabled worker, schedule a wake
    /// (debounced, so a burst of mail is one wake).
    fn maybe_wake(&mut self, agent: &str, env: &Amf) -> Result<()> {
        if self.wake_enabled(agent).is_none() {
            return Ok(());
        }
        // Never wake (now or as a follow-up) for a thread the loop guard paused.
        let paused: Option<String> = self.db.query_row("SELECT reason FROM paused WHERE key=?1", params![format!("thread:{}", env.thread_id)], |r| r.get(0)).optional()?;
        if paused.is_some() {
            return Ok(());
        }
        // A wake is running, or just finished and its session hasn't closed yet: this mail
        // gets a follow-up wake once that session ends (unless it handles it first).
        let recent = self.wake_finished.get(agent).is_some_and(|t| t.elapsed() < Duration::from_secs(15));
        if self.wake_inflight.contains_key(agent) || (recent && self.sessions.contains_key(agent)) {
            self.wake_again.insert(agent.to_string());
            return Ok(());
        }
        if self.sessions.contains_key(agent) {
            return Ok(()); // a live session will handle it
        }
        let due = Instant::now() + WakeLimits::from_env().debounce;
        self.wake_due.entry(agent.to_string()).or_insert(due);
        Ok(())
    }

    fn record_wake(&self, agent: &str, rev: Option<u64>, outcome: &str, seconds: f64, session: Option<Option<String>>) {
        let rec = crate::wake::WakeRecord { at: now().to_rfc3339(), outcome: outcome.to_string(), seconds };
        let r = crate::update_wake(&self.home, |wf| {
            // Only into the configuration this wake ran with (not one re-enabled meanwhile).
            if let Some(c) = wf.workers.get_mut(agent).filter(|c| rev.is_none_or(|r| c.rev == r)) {
                c.last = Some(rec);
                if let Some(sid) = session {
                    c.session_id = sid;
                }
            }
            Ok(())
        });
        if let Err(e) = r {
            warn!("wake: could not record the outcome for {agent}: {e:#}");
        }
    }

    fn fire_wakes(&mut self) -> Result<()> {
        if self.wake_due.is_empty() {
            return Ok(());
        }
        let limits = WakeLimits::from_env();
        let now_i = Instant::now();
        let mut due: Vec<(String, Instant)> = self.wake_due.iter().filter(|(_, t)| **t <= now_i).map(|(a, t)| (a.clone(), *t)).collect();
        due.sort_by_key(|(_, t)| *t);
        for (agent, _) in due {
            if self.wake_inflight.len() >= limits.concurrent {
                break; // stays due; fires when a slot frees up
            }
            self.wake_due.remove(&agent);
            let Some(p) = self.wake_enabled(&agent) else { continue };
            if self.sessions.contains_key(&agent) || self.wake_inflight.contains_key(&agent) {
                continue;
            }
            let unread = self.unread_count(&agent)?;
            if unread == 0 {
                continue;
            }
            let Some(cfg) = crate::load_wake(&self.home).ok().and_then(|w| w.workers.get(&agent).cloned()) else { continue };
            let hour = now().timestamp() / 3600;
            let key = format!("wake:{agent}");
            if self.budget_used(&key, hour)? >= limits.per_hour {
                warn!("wake: {} skipped: {} wakes this hour (AGENTLAN_WAKE_PER_HOUR)", self.handle_of(&p), limits.per_hour);
                self.record_wake(&agent, None, &format!("skipped: reached {} wakes this hour", limits.per_hour), 0.0, None);
                continue;
            }
            self.db.execute("INSERT INTO budget(key, window, count) VALUES(?1,?2,1) ON CONFLICT(key, window) DO UPDATE SET count=count+1", params![key, hour])?;
            let latest: Option<Amf> = self
                .db
                .query_row(
                    "SELECT m.envelope FROM inbox i JOIN messages m USING(msg_id) WHERE i.agent_id=?1 AND i.state!='acked' ORDER BY i.rowid DESC LIMIT 1",
                    params![agent],
                    |r| r.get::<_, String>(0),
                )
                .optional()?
                .and_then(|e| serde_json::from_str(&e).ok());
            let job = crate::wake::WakeJob {
                agent_id: agent.clone(),
                name: p.name.clone(),
                handle: self.handle_of(&p),
                role: p.role.clone(),
                home: self.home.clone(),
                cfg,
                unread,
                latest_from: latest.as_ref().and_then(|m| m.from.handle.clone()),
                latest_intent: latest.as_ref().map(|m| m.intent.clone()),
            };
            info!("wake: starting {} ({unread} unread)", job.handle);
            let tx = self.cmd_tx.clone();
            let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("agentlan"));
            let id = agent.clone();
            let h = self.rt.spawn(async move {
                let result = crate::wake::run(job, exe).await;
                let _ = tx.send(NCmd::WakeDone { agent_id: id, result });
            });
            self.wake_inflight.insert(agent, (now_i, h.abort_handle()));
        }
        Ok(())
    }

    fn wake_done(&mut self, agent: &str, result: crate::wake::WakeResult) -> Result<()> {
        self.wake_inflight.remove(agent);
        self.wake_finished.insert(agent.to_string(), Instant::now());
        self.record_wake(agent, Some(result.rev), &result.outcome, result.seconds, Some(result.session_id.clone()));
        // The wake's MCP session closes on its own schedule: if it's still open, the
        // follow-up waits for end_session.
        if !self.sessions.contains_key(agent) {
            self.wake_followup(agent)?;
        }
        Ok(())
    }

    /// Mail that arrived while a wake ran gets another wake (budget permitting); mail it
    /// left unread from before doesn't (no wake loops on a message it won't handle).
    fn wake_followup(&mut self, agent: &str) -> Result<()> {
        if !self.wake_inflight.contains_key(agent) && self.wake_again.remove(agent) && self.unread_count(agent)? > 0 {
            self.wake_due.insert(agent.to_string(), Instant::now() + WakeLimits::from_env().debounce);
        }
        Ok(())
    }

    fn pair_paused(&self, sender: &str, peer: &str) -> Result<Option<String>> {
        Ok(self.db.query_row("SELECT reason FROM paused WHERE key=?1", params![format!("pair:{sender}>{peer}")], |r| r.get(0)).optional()?)
    }

    fn budget_used(&self, key: &str, window: i64) -> Result<i64> {
        Ok(self.db.query_row("SELECT COALESCE(SUM(count),0) FROM budget WHERE key=?1 AND window=?2", params![key, window], |r| r.get(0))?)
    }

    fn pause(&self, key: &str, reason: &str) -> Result<()> {
        self.db.execute("INSERT OR IGNORE INTO paused(key, paused_at, reason) VALUES(?1,?2,?3)", params![key, now().to_rfc3339(), reason])?;
        warn!("node: loop guard paused {key}");
        Ok(())
    }

    /// Remote recipients the hub committed: allowed repliers for requests, and pair
    /// accounting for group sends (direct sends were debited at commit).
    fn record_remote_recipients(&mut self, msg_id: &str, recipients: &[String]) -> Result<()> {
        if recipients.is_empty() {
            return Ok(());
        }
        let row: Option<(String, String)> = self
            .db
            .query_row("SELECT s.sender, m.envelope FROM sent s JOIN messages m USING(msg_id) WHERE s.msg_id=?1", params![msg_id], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?;
        let Some((sender, env)) = row else {
            return Ok(());
        };
        let env: Amf = serde_json::from_str(&env)?;
        let limits = Limits::from_env();
        let hour = now().timestamp() / 3600;
        let tx = self.db.unchecked_transaction()?;
        for r in recipients {
            tx.execute("INSERT OR IGNORE INTO request_recipients(msg_id, agent_id) VALUES(?1,?2)", params![msg_id, r])?;
        }
        tx.commit()?;
        if !env.to.is_direct() {
            for r in recipients {
                let key = format!("pair:{sender}>{r}");
                self.db.execute("INSERT INTO budget(key, window, count) VALUES(?1,?2,1) ON CONFLICT(key, window) DO UPDATE SET count=count+1", params![key, hour])?;
                if self.budget_used(&key, hour)? >= limits.per_pair_hour {
                    self.pause(&key, &format!("reached {} messages per hour to this recipient", limits.per_pair_hour))?;
                }
            }
        }
        Ok(())
    }

    /// Validate, apply the loop guard and commit a send in one transaction.
    /// `Err(dup)` = an identical earlier send (retry key), returned as-is.
    fn commit_send(&mut self, conn: u64, as_agent: Option<&str>, mut draft: SendDraft) -> R<std::result::Result<Committed, (String, Value)>> {
        let sender = self.agent_for(conn, as_agent)?;
        // Caller retry keys are scoped to the sender.
        let client_id = draft.id.take();
        if let Some(cid) = &client_id {
            let canon = canonical_msg_id(&sender.agent_id, cid);
            let prev: Option<String> = self.db.query_row("SELECT result FROM sent WHERE msg_id=?1", params![canon], |r| r.get(0)).optional()?;
            if let Some(p) = prev {
                let mut res: SendResult = serde_json::from_str(&p)?;
                res.duplicate = true;
                return Ok(Err((canon, serde_json::to_value(res)?)));
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
                    None => {
                        return Err(IpcErr("UNKNOWN_RECIPIENT", format!("no agent '{n}' on this machine")));
                    }
                },
                Some(_) => (vec![], true),
                None => return Err(IpcErr("INVALID", format!("bad handle '{handle}'"))),
            },
            Target::Role { role, project, .. } => {
                let c: Vec<AgentProfile> = self
                    .profiles
                    .iter()
                    .filter(|p| attached_match(p, &|p| &p.role == role && project.as_ref().is_none_or(|pr| agentlan_proto::ids::project_matches(p.project.as_deref(), pr))))
                    .cloned()
                    .collect();
                let c = match project {
                    Some(pr) => agentlan_proto::ids::prefer_exact_project(c, pr, |p| p.project.as_deref()),
                    None => c,
                };
                (c, true)
            }
            Target::Project { project, .. } => {
                let c: Vec<AgentProfile> =
                    self.profiles.iter().filter(|p| attached_match(p, &|p| agentlan_proto::ids::project_matches(p.project.as_deref(), project))).cloned().collect();
                (agentlan_proto::ids::prefer_exact_project(c, project, |p| p.project.as_deref()), true)
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

        let limits = Limits::from_env();
        let hour = now().timestamp() / 3600;
        let direct = env.to.is_direct();
        // Group sends: skip local recipients whose pair is paused or would exceed its budget.
        if !direct {
            let mut keep = Vec::new();
            for p in local {
                let key = format!("pair:{}>{}", sender.agent_id, p.agent_id);
                if self.pair_paused(&sender.agent_id, &p.agent_id)?.is_some() {
                    continue;
                }
                if self.budget_used(&key, hour)? >= limits.per_pair_hour {
                    self.pause(&key, &format!("reached {} messages per hour to this recipient", limits.per_pair_hour))?;
                    continue;
                }
                keep.push(p);
            }
            local = keep;
        }
        // Single-recipient sends (e.g. `request` to a role) with nobody live to take them:
        // fall back to a wake-enabled worker here, unless the roster shows a live match on
        // another PC (the hub applies the same fallback for remote workers).
        if matches!(&env.to, Target::Role { pick: Pick::One, .. } | Target::Project { pick: Pick::One, .. }) && local.is_empty() {
            let matches = |p: &AgentProfile| match &env.to {
                Target::Role { role, project, .. } => &p.role == role && project.as_ref().is_none_or(|pr| agentlan_proto::ids::project_matches(p.project.as_deref(), pr)),
                Target::Project { project, .. } => agentlan_proto::ids::project_matches(p.project.as_deref(), project),
                _ => false,
            };
            let live_remote = self.base_agents.values().any(|a| {
                a.machine_id != self.cfg.machine_id
                    && a.state == PresenceState::Online
                    && matches(&a.profile)
                    && self.pair_paused(&sender.agent_id, &a.profile.agent_id).ok().flatten().is_none()
            });
            if !live_remote {
                let mut cands: Vec<AgentProfile> =
                    self.profiles.iter().filter(|p| p.agent_id != sender.agent_id && matches(p) && self.wake_enabled(&p.agent_id).is_some()).cloned().collect();
                cands.retain(|p| self.pair_paused(&sender.agent_id, &p.agent_id).ok().flatten().is_none());
                let want = match &env.to {
                    Target::Role { project: Some(pr), .. } | Target::Project { project: pr, .. } => Some(pr.clone()),
                    _ => None,
                };
                if let Some(w) = want {
                    cands = agentlan_proto::ids::prefer_exact_project(cands, &w, |p| p.project.as_deref());
                }
                if let Some(p) = cands.into_iter().next() {
                    local = vec![p];
                }
            }
        }
        if matches!(&env.to, Target::Role { pick: Pick::One, .. } | Target::Project { pick: Pick::One, .. }) && !local.is_empty() {
            local.truncate(1);
            needs_remote = false; // a committed local pick skips the remote leg
        }
        let local_ids: Vec<String> = local.iter().map(|p| p.agent_id.clone()).collect();

        // Direct sends: one canonical pair key, however the peer was addressed.
        let direct_peer: Option<String> = match &env.to {
            Target::Agent { agent } => Some(agent.clone()),
            Target::Handle { handle } => {
                let h = handle.trim_start_matches('@');
                local
                    .first()
                    .map(|p| p.agent_id.clone())
                    .or_else(|| self.base_agents.values().find(|a| a.handle == h).map(|a| a.profile.agent_id.clone()))
                    .or_else(|| Some(h.to_string()))
            }
            _ => None,
        };
        let pair_key = direct_peer.as_ref().map(|peer| format!("pair:{}>{peer}", sender.agent_id));
        let thread_key = format!("thread:{}", env.thread_id);
        for key in std::iter::once(&thread_key).chain(pair_key.iter()) {
            let paused: Option<String> = self.db.query_row("SELECT reason FROM paused WHERE key=?1", params![key], |r| r.get(0)).optional()?;
            if let Some(reason) = paused {
                return Err(IpcErr("PAUSED", format!("{key} is paused ({reason}); a human must run `agentlan thread resume {key}`")));
            }
        }
        let mut debits: Vec<(String, i64)> = vec![(format!("agent:{}", sender.agent_id), hour), (thread_key.clone(), 0)];
        if let Some(k) = &pair_key {
            debits.push((k.clone(), hour));
        }
        if !direct {
            for id in &local_ids {
                debits.push((format!("pair:{}>{id}", sender.agent_id), hour));
            }
        }
        for (key, window) in &debits {
            let used = self.budget_used(key, *window)?;
            let (limit, what) = if key.starts_with("agent:") {
                (limits.per_agent_hour, "messages per hour from this agent")
            } else if key.starts_with("pair:") {
                (limits.per_pair_hour, "messages per hour to this recipient")
            } else {
                (limits.per_thread, "automated messages in this thread")
            };
            if used >= limit {
                // Threads and pairs pause until a human resumes them (this is what stops a
                // ping-pong); the per-agent hourly cap is a plain rate limit.
                if key.starts_with("thread:") || key.starts_with("pair:") {
                    self.pause(key, &format!("reached {limit} {what}"))?;
                    return Err(IpcErr("PAUSED", format!("{key} reached {limit} {what}; it is now paused — a human must run `agentlan thread resume {key}`")));
                }
                return Err(IpcErr("RATE_LIMITED", format!("loop guard: {limit} {what} reached; try again later")));
            }
        }
        // Remote leg: tell the hub which of this sender's pairs are paused.
        let exclude: Vec<String> = {
            let mut st = self.db.prepare("SELECT key FROM paused WHERE key LIKE ?1")?;
            let prefix = format!("pair:{}>", sender.agent_id);
            let keys: Vec<String> = st.query_map(params![format!("{prefix}%")], |r| r.get(0))?.collect::<Result<_, _>>()?;
            keys.into_iter().filter_map(|k| k.strip_prefix(&prefix).map(String::from)).collect()
        };

        let result = SendResult {
            id: env.id.clone(),
            thread_id: env.thread_id.clone(),
            local: local.len() as u32,
            remote: if needs_remote { RemoteResult::Queued } else { RemoteResult::None },
            duplicate: false,
            client_id,
        };

        // One transaction: message + local inbox rows + dedup record + budgets + deferred work.
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
                tx.execute("INSERT OR IGNORE INTO request_recipients(msg_id, agent_id) VALUES(?1,?2)", params![env.id, id])?;
            }
            if let Some(peer) = direct_peer.as_ref().filter(|p| p.starts_with("agt_")) {
                tx.execute("INSERT OR IGNORE INTO request_recipients(msg_id, agent_id) VALUES(?1,?2)", params![env.id, peer])?;
            }
            tx.execute("INSERT INTO sent(msg_id, sender, result) VALUES(?1,?2,?3)", params![env.id, sender.agent_id, serde_json::to_string(&result)?])?;
            for (key, window) in &debits {
                tx.execute(
                    "INSERT INTO budget(key, window, count) VALUES(?1,?2,1) ON CONFLICT(key, window) DO UPDATE SET count=count+1",
                    params![key, window],
                )?;
            }
            if needs_remote {
                tx.execute(
                    "INSERT INTO outbox(kind, msg_id, body) VALUES('submit', ?1, ?2)",
                    params![env.id, json!({"agentId": sender.agent_id, "envelope": env, "localRecipients": local_ids, "exclude": exclude}).to_string()],
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
        Ok(Ok(Committed { env, result, needs_remote, local_ids }))
    }

    fn deliver_local(&mut self, c: &Committed) -> Result<()> {
        for id in &c.local_ids {
            self.notify(id, &c.env)?;
        }
        self.pump()
    }

    fn send(&mut self, conn: u64, req_id: u64, as_agent: Option<&str>, draft: SendDraft) -> R<Option<Value>> {
        let c = match self.commit_send(conn, as_agent, draft)? {
            Ok(c) => c,
            Err((_, dup)) => return Ok(Some(dup)),
        };
        self.deliver_local(&c)?;
        if c.needs_remote && self.hub == HubStatus::Ready {
            // Answer when the hub decides (or after SEND_WAIT as "queued").
            self.pending.insert(c.env.id.clone(), PendingSend { conn, req: req_id, result: c.result, deadline: Instant::now() + SEND_WAIT });
            return Ok(None);
        }
        Ok(Some(serde_json::to_value(c.result)?))
    }

    /// Send + wait for the reply as one step: the reply-waiter exists before any reply
    /// can be processed, and only the actual recipient's reply satisfies it.
    fn request(&mut self, conn: u64, req_id: u64, as_agent: Option<&str>, mut draft: SendDraft, timeout_ms: u64) -> R<Option<Value>> {
        draft.to = match draft.to.take() {
            Some(Target::Role { role, project, .. }) => Some(Target::Role { role, project, pick: Pick::One }),
            Some(Target::Project { project, .. }) => Some(Target::Project { project, pick: Pick::One }),
            Some(t @ (Target::Agent { .. } | Target::Handle { .. })) => Some(t),
            Some(_) => return Err(IpcErr("INVALID", "a request needs a single recipient: @name@machine, agt_…, role:R or project:P".into())),
            None => return Err(IpcErr("INVALID", "missing 'to'".into())),
        };
        if draft.intent.is_none() {
            draft.intent = Some("request".into());
        }
        let sender = self.agent_for(conn, as_agent)?.agent_id;
        let (id, result, committed) = match self.commit_send(conn, as_agent, draft)? {
            Ok(c) => {
                if c.local_ids.is_empty() && !c.needs_remote {
                    return Err(IpcErr("NOT_DELIVERED", "no agent matched the request (role/project targets need an online agent)".into()));
                }
                (c.env.id.clone(), c.result.clone(), Some(c))
            }
            Err((id, dup)) => (id, serde_json::from_value(dup)?, None),
        };
        // Register the reply-waiter before anything else can run.
        self.waiters.push(Waiter {
            conn,
            req: req_id,
            agent: sender.clone(),
            deadline: Instant::now() + Duration::from_millis(timeout_ms.min(300_000)),
            in_reply_to: Some(id.clone()),
            request: Some(result),
        });
        if let Some(c) = committed {
            self.deliver_local(&c)?;
        } else {
            // Retry of an earlier request: its reply may already be here.
            let have: Option<String> = self
                .db
                .query_row(
                    "SELECT m.envelope FROM inbox i JOIN messages m USING(msg_id) WHERE i.agent_id=?1 AND i.state!='acked'
                       AND json_extract(m.envelope,'$.inReplyTo')=?2
                       AND json_extract(m.envelope,'$.from.agent') IN (SELECT agent_id FROM request_recipients WHERE msg_id=?2) ORDER BY i.rowid LIMIT 1",
                    params![sender, id],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(env) = have {
                let env: Amf = serde_json::from_str(&env)?;
                self.offer_to_waiters(&sender, &env)?;
            }
        }
        Ok(None)
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
