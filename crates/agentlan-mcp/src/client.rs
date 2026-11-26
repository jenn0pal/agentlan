//! A long-lived IPC client to the local node. Each identity in an adapter (the main
//! agent, each subagent) has its own client, i.e. its own attached node session.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use agentlan_proto::ipc::{IpcError, IpcOp, IpcOut, IpcRequest};
use anyhow::{Result, anyhow};
use interprocess::local_socket::tokio::{Stream, prelude::*};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, oneshot};

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<std::result::Result<Value, IpcError>>>>>;

#[derive(Clone)]
struct Live {
    stream: Arc<Stream>,
    pending: Pending,
    alive: Arc<AtomicBool>,
    /// Serialises line writes so concurrent calls can't interleave bytes.
    wlock: Arc<Mutex<()>>,
    reader: tokio::task::AbortHandle,
}

struct AbortOnDrop(Option<tokio::task::AbortHandle>);

impl AbortOnDrop {
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(h) = self.0.take() {
            h.abort();
        }
    }
}

/// What a client's connection is bound to on the node.
#[derive(Clone, Debug)]
pub enum Bind {
    /// Not an agent session (roster lookups before registering).
    None,
    /// An existing profile, by name (`agentlan mcp --agent <name>`).
    Attach(String),
    /// A self-assigned identity (the `register` tool).
    Register(RegisterArgs),
}

#[derive(Clone, Debug, Default)]
pub struct RegisterArgs {
    pub name: Option<String>,
    pub role: String,
    pub project: Option<String>,
    pub description: Option<String>,
}

async fn write_line(stream: &Stream, wlock: &Mutex<()>, line: &[u8]) -> std::io::Result<()> {
    let _g = wlock.lock().await;
    let mut w = stream;
    w.write_all(line).await
}

/// Where new-mail pushes for an identity go (the adapter's live delivery): (name, data).
pub type PushSink = tokio::sync::mpsc::UnboundedSender<(String, Value)>;

pub struct NodeClient {
    home: PathBuf,
    sink: Arc<std::sync::Mutex<Option<(String, PushSink)>>>,
    bind: std::sync::Mutex<Bind>,
    runner: String,
    host_pid: Option<u32>,
    live: Mutex<Option<Live>>,
    next: AtomicU64,
    /// Set when the node tells us this session was taken over or rejected.
    pub ended: Arc<Mutex<Option<String>>>,
}

#[derive(Debug)]
pub struct CallError {
    pub code: String,
    pub msg: String,
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.msg)
    }
}

impl NodeClient {
    pub fn new(home: PathBuf, bind: Bind, runner: String, host_pid: Option<u32>) -> NodeClient {
        NodeClient {
            home,
            sink: Arc::new(std::sync::Mutex::new(None)),
            bind: std::sync::Mutex::new(bind),
            runner,
            host_pid,
            live: Mutex::new(None),
            next: AtomicU64::new(1),
            ended: Arc::new(Mutex::new(None)),
        }
    }

    async fn open(&self) -> Result<(Live, Value)> {
        // A PC that has joined gets its node started on demand (it keeps running after
        // this session ends); one that hasn't must `connect` with a join code first.
        if !agentlan_node::is_running(&self.home).await {
            if agentlan_node::load_config(&self.home).is_err() {
                anyhow::bail!(
                    "NOT_JOINED: this PC hasn't joined an AgentLAN network yet; call `connect` with the join code from the server (`agentlan up` prints it)"
                );
            }
            agentlan_node::ensure_running(&self.home).await.map_err(|e| anyhow!("NODE_DOWN: {e:#}"))?;
        }
        let name = agentlan_node::ipc_name(&self.home)?;
        let stream = Arc::new(Stream::connect(name).await.map_err(|e| anyhow!("NODE_DOWN: the agentlan node is not running ({e})"))?);
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let alive = Arc::new(AtomicBool::new(true));
        let wlock = Arc::new(Mutex::new(()));
        let reader = {
            let (stream, pending, alive, ended, wlock) = (stream.clone(), pending.clone(), alive.clone(), self.ended.clone(), wlock.clone());
            let sink = self.sink.clone();
            tokio::spawn(async move {
                let mut rd = BufReader::new(&*stream);
                let mut line = String::new();
                loop {
                    line.clear();
                    match rd.read_line(&mut line).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                    match serde_json::from_str::<IpcOut>(line.trim()) {
                        Ok(IpcOut::Response(r)) => {
                            if let Some(tx) = pending.lock().await.remove(&r.id) {
                                let res = if r.ok {
                                    Ok(r.result.unwrap_or(Value::Null))
                                } else {
                                    Err(r.error.unwrap_or(IpcError { code: "ERROR".into(), msg: "unknown".into() }))
                                };
                                // The caller gave up (e.g. the MCP request was cancelled) but a
                                // message was already handed over: put it back as unseen.
                                if let Err(Ok(v)) = tx.send(res) {
                                    if let Some(mid) = v["message"]["id"].as_str() {
                                        let op = IpcRequest { id: 0, op: IpcOp::Unsurface { agent: None, message_id: mid.to_string() } };
                                        if let Ok(mut line) = serde_json::to_vec(&op) {
                                            line.push(b'\n');
                                            let _ = write_line(&stream, &wlock, &line).await;
                                        }
                                    }
                                }
                            }
                        }
                        Ok(IpcOut::Push(p)) if p.event == "message" => {
                            let s = sink.lock().unwrap().clone();
                            if let Some((name, tx)) = s {
                                let _ = tx.send((name, p.data));
                            }
                        }
                        Ok(IpcOut::Push(p)) if p.event == "session" => {
                            let status = p.data["status"].as_str().unwrap_or("");
                            if status == "ended" || status == "rejected" {
                                *ended.lock().await = Some(format!("session {status}: {}", p.data["reason"].as_str().unwrap_or("")));
                            }
                        }
                        _ => {}
                    }
                }
                alive.store(false, Ordering::SeqCst);
                for (_, tx) in pending.lock().await.drain() {
                    let _ = tx.send(Err(IpcError { code: "NODE_DOWN".into(), msg: "connection to the node closed".into() }));
                }
            })
            .abort_handle()
        };
        let live = Live { stream, pending, alive, wlock, reader };
        // If this future is dropped mid-bind (the host cancelled the call), close the
        // half-open connection instead of leaving a session the adapter doesn't know about.
        let guard = AbortOnDrop(Some(live.reader.clone()));
        let bind = self.bind.lock().unwrap().clone();
        let pid = Some(std::process::id());
        let op = match &bind {
            Bind::None => {
                guard.disarm();
                return Ok((live, Value::Null));
            }
            Bind::Attach(agent) => IpcOp::Attach { agent: agent.clone(), runner: Some(self.runner.clone()), pid, takeover: false, host_pid: self.host_pid },
            Bind::Register(a) => IpcOp::Register {
                name: a.name.clone(),
                role: a.role.clone(),
                project: a.project.clone(),
                description: a.description.clone(),
                runner: Some(self.runner.clone()),
                pid,
                host_pid: self.host_pid,
            },
        };
        let attached = match self.raw(&live, op).await {
            Ok(v) => v,
            Err(e) => anyhow::bail!("{}: {}", e.code, e.msg), // guard closes the connection
        };
        guard.disarm();
        // After a node restart, re-register under the name we got (keeps the recorded id).
        if let Bind::Register(mut a) = bind
            && let Some(n) = attached["agent"]["name"].as_str()
        {
            a.name = Some(n.to_string());
            *self.bind.lock().unwrap() = Bind::Register(a);
        }
        Ok((live, attached))
    }

    /// Forward this identity's new-mail pushes to the adapter's live delivery.
    pub fn set_push_sink(&self, name: &str, tx: PushSink) {
        *self.sink.lock().unwrap() = Some((name.to_string(), tx));
    }

    /// Update the args used if this identity has to re-register after a node restart.
    pub fn set_register_args(&self, a: RegisterArgs) {
        let mut b = self.bind.lock().unwrap();
        if matches!(*b, Bind::Register(_)) {
            *b = Bind::Register(a);
        }
    }

    /// End this identity's session (the agent left) and close the connection.
    pub async fn close(&self) {
        let _ = self.call(IpcOp::Detach).await;
        *self.ended.lock().await = Some("session ended: left".into());
        if let Some(l) = self.live.lock().await.take() {
            l.reader.abort();
        }
    }

    async fn raw(&self, live: &Live, op: IpcOp) -> std::result::Result<Value, IpcError> {
        let id = self.reserve_id();
        self.raw_id(live, id, op).await
    }

    /// Reserve an IPC request id (so a later CancelWait can name exactly this call).
    pub fn reserve_id(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    async fn raw_id(&self, live: &Live, id: u64, op: IpcOp) -> std::result::Result<Value, IpcError> {
        let (tx, rx) = oneshot::channel();
        live.pending.lock().await.insert(id, tx);
        let mut line = serde_json::to_vec(&IpcRequest { id, op }).map_err(|e| IpcError { code: "INTERNAL".into(), msg: e.to_string() })?;
        line.push(b'\n');
        if let Err(e) = write_line(&live.stream, &live.wlock, &line).await {
            live.pending.lock().await.remove(&id);
            return Err(IpcError { code: "NODE_DOWN".into(), msg: e.to_string() });
        }
        rx.await.unwrap_or_else(|_| Err(IpcError { code: "NODE_DOWN".into(), msg: "connection closed".into() }))
    }

    /// Open the connection and bind it now; returns the node's attach/register result.
    pub async fn connect(&self) -> Result<Value> {
        let (live, attached) = self.open().await?;
        *self.live.lock().await = Some(live);
        Ok(attached)
    }

    /// Call an op on the attached session, reconnecting (and re-attaching) if the node restarted.
    pub async fn call(&self, op: IpcOp) -> std::result::Result<Value, CallError> {
        let id = self.reserve_id();
        self.call_with_id(id, op).await
    }

    pub async fn call_with_id(&self, id: u64, op: IpcOp) -> std::result::Result<Value, CallError> {
        if let Some(why) = self.ended.lock().await.clone() {
            return Err(CallError { code: "SESSION_ENDED".into(), msg: why });
        }
        // Hold the lock only to (re)connect; long waits must not block other calls.
        let live = {
            let mut guard = self.live.lock().await;
            if guard.as_ref().is_none_or(|l| !l.alive.load(Ordering::SeqCst)) {
                match self.open().await {
                    Ok((l, _)) => *guard = Some(l),
                    Err(e) => {
                        let s = e.to_string();
                        let (code, msg) = s.split_once(": ").map(|(a, b)| (a.to_string(), b.to_string())).unwrap_or(("NODE_DOWN".into(), s));
                        return Err(CallError { code, msg });
                    }
                }
            }
            guard.as_ref().unwrap().clone()
        };
        self.raw_id(&live, id, op).await.map_err(|e| CallError { code: e.code, msg: e.msg })
    }
}
