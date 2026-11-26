//! A long-lived IPC client to the local node that holds one attached session.

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
}

async fn write_line(stream: &Stream, wlock: &Mutex<()>, line: &[u8]) -> std::io::Result<()> {
    let _g = wlock.lock().await;
    let mut w = stream;
    w.write_all(line).await
}

pub struct NodeClient {
    home: PathBuf,
    agent: String,
    runner: String,
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
    pub fn new(home: PathBuf, agent: String, runner: String) -> NodeClient {
        NodeClient { home, agent, runner, live: Mutex::new(None), next: AtomicU64::new(1), ended: Arc::new(Mutex::new(None)) }
    }

    async fn open(&self) -> Result<(Live, Value)> {
        let name = agentlan_node::ipc_name(&self.home)?;
        let stream = Arc::new(Stream::connect(name).await.map_err(|e| anyhow!("NODE_DOWN: the agentlan node is not running ({e})"))?);
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let alive = Arc::new(AtomicBool::new(true));
        let wlock = Arc::new(Mutex::new(()));
        {
            let (stream, pending, alive, ended, wlock) = (stream.clone(), pending.clone(), alive.clone(), self.ended.clone(), wlock.clone());
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
            });
        }
        let live = Live { stream, pending, alive, wlock };
        let attached = self
            .raw(&live, IpcOp::Attach { agent: self.agent.clone(), runner: Some(self.runner.clone()), pid: Some(std::process::id()), takeover: false })
            .await
            .map_err(|e| anyhow!("{}: {}", e.code, e.msg))?;
        Ok((live, attached))
    }

    async fn raw(&self, live: &Live, op: IpcOp) -> std::result::Result<Value, IpcError> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
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

    /// Attach now (used at startup to build the initial instructions).
    pub async fn connect(&self) -> Result<Value> {
        let (live, attached) = self.open().await?;
        *self.live.lock().await = Some(live);
        Ok(attached)
    }

    /// Call an op on the attached session, reconnecting (and re-attaching) if the node restarted.
    pub async fn call(&self, op: IpcOp) -> std::result::Result<Value, CallError> {
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
        self.raw(&live, op).await.map_err(|e| CallError { code: e.code, msg: e.msg })
    }
}
