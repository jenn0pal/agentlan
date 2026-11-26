//! `ailink` — one binary: hub, node daemon and CLI client.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ailink_proto::amf::{SendDraft, Target};
use ailink_proto::ipc::{IpcOp, IpcOut, IpcRequest};
use ailink_proto::join::JoinCode;
use ailink_proto::records::AgentMode;
use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use interprocess::local_socket::tokio::{Stream, prelude::*};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Parser)]
#[command(name = "ailink", version, about = "Link AI agents across PCs on your LAN")]
struct Cli {
    /// Data directory (default: per-user app data; env AILINK_HOME).
    #[arg(long, global = true, env = "AILINK_HOME")]
    home: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Server (hub) commands — run them on whichever PC you pick as the server.
    Hub {
        #[command(subcommand)]
        cmd: HubCmd,
    },
    /// Join this PC to a network using a join code from `ailink hub invite`.
    Join {
        code: String,
        /// Machine alias (default: hostname).
        #[arg(long)]
        alias: Option<String>,
    },
    /// Node daemon commands.
    Node {
        #[command(subcommand)]
        cmd: NodeCmd,
    },
    /// Agent profiles on this PC.
    Agent {
        #[command(subcommand)]
        cmd: AgentCmd,
    },
    /// Node status.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// List agents (this PC and the network).
    Ls {
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        role: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// List machines.
    Machines {
        #[arg(long)]
        json: bool,
    },
    /// Attach as an agent and stream its events until interrupted.
    Attach {
        agent: String,
        #[arg(long)]
        takeover: bool,
        #[arg(long)]
        json: bool,
    },
    /// Stream events without attaching.
    Watch {
        #[arg(long = "as")]
        as_agent: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Send a message. TARGET: @name@machine | agt_… | role:R[@project] | project:P | machine:M | all.
    /// With --stdin, read an AMF draft (JSON) from stdin.
    Send {
        #[arg(long = "as")]
        as_agent: String,
        target: Option<String>,
        text: Option<String>,
        #[arg(long)]
        subject: Option<String>,
        #[arg(long)]
        intent: Option<String>,
        /// Message id for idempotent retries.
        #[arg(long)]
        id: Option<String>,
        #[arg(long = "reply-to")]
        reply_to: Option<String>,
        #[arg(long)]
        stdin: bool,
        #[arg(long)]
        json: bool,
    },
    /// Show un-acked messages.
    Inbox {
        #[arg(long = "as")]
        as_agent: String,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        json: bool,
    },
    /// Acknowledge (mark handled) a message.
    Ack {
        #[arg(long = "as")]
        as_agent: String,
        message_id: String,
    },
    /// Wait for the next message.
    Wait {
        #[arg(long = "as")]
        as_agent: String,
        #[arg(long, default_value_t = 30)]
        timeout: u64,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum HubCmd {
    /// Create the server identity on this PC.
    Init {
        #[arg(long, default_value = "home-lab")]
        name: String,
        #[arg(long, default_value = "0.0.0.0:47820")]
        listen: String,
    },
    /// Print a one-time join code.
    Invite {
        /// Address(es) other PCs should use to reach this server (default: this PC's LAN IPs).
        #[arg(long)]
        addr: Vec<String>,
        #[arg(long, default_value_t = 15)]
        ttl_min: u64,
    },
    /// Run the server.
    Run {
        #[arg(long)]
        listen: Option<String>,
    },
}

#[derive(Subcommand)]
enum NodeCmd {
    /// Run the node daemon.
    Run,
}

#[derive(Subcommand)]
enum AgentCmd {
    /// Register an agent on this PC (gets a permanent agt_… id).
    Add {
        name: String,
        #[arg(long)]
        role: String,
        #[arg(long)]
        project: Option<String>,
        #[arg(long, default_value = "generic")]
        runner: String,
        #[arg(long, default_value = "interactive")]
        mode: String,
    },
    /// List agent profiles on this PC.
    Ls {
        #[arg(long)]
        json: bool,
    },
}

fn default_home() -> PathBuf {
    directories::ProjectDirs::from("dev", "ailink", "ailink").map(|d| d.data_dir().to_path_buf()).unwrap_or_else(|| PathBuf::from(".ailink"))
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::try_from_env("AILINK_LOG").unwrap_or_else(|_| "warn".into())).with_writer(std::io::stderr).init();
    let home = cli.home.clone().unwrap_or_else(default_home);
    if let Err(e) = run(cli.cmd, &home).await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

async fn run(cmd: Cmd, home: &Path) -> Result<()> {
    match cmd {
        Cmd::Hub { cmd } => match cmd {
            HubCmd::Init { name, listen } => {
                let info = ailink_hub::init(home, &name, &listen)?;
                println!("✓ server created in {}\n  key fingerprint {}\n  listens on {}", ailink_hub::hub_dir(home).display(), info.fingerprint, info.listen);
                println!("next: `ailink hub run`, then `ailink hub invite` for each PC");
            }
            HubCmd::Invite { addr, ttl_min } => {
                let addrs = if addr.is_empty() {
                    let hf = ailink_hub::load(home)?;
                    let port = hf.listen.rsplit(':').next().unwrap_or("47820").to_string();
                    let ips = ailink_node::machine_info(false).addrs;
                    if ips.is_empty() {
                        bail!("no LAN address found; pass --addr HOST:PORT");
                    }
                    ips.into_iter().map(|ip| format!("{ip}:{port}")).collect()
                } else {
                    addr
                };
                let code = ailink_hub::invite(home, addrs, Duration::from_secs(ttl_min * 60))?;
                println!("{}", code.encode());
            }
            HubCmd::Run { listen } => ailink_hub::run(home, listen).await?,
        },
        Cmd::Join { code, alias } => {
            let code = JoinCode::decode(&code).map_err(|e| anyhow!(e))?;
            let alias = alias.unwrap_or_else(|| ailink_node::machine_info(false).hostname);
            let (mid, alias) = ailink_node::join(home, &code, &alias).await?;
            println!("✓ joined network '{}' as {alias} ({mid})\nnext: `ailink agent add …`, then `ailink node run`", code.net);
        }
        Cmd::Node { cmd: NodeCmd::Run } => ailink_node::run(home).await?,
        Cmd::Agent { cmd } => match cmd {
            AgentCmd::Add { name, role, project, runner, mode } => {
                let mode = match mode.as_str() {
                    "interactive" => AgentMode::Interactive,
                    "worker" => AgentMode::Worker,
                    "script" => AgentMode::Script,
                    m => bail!("unknown mode '{m}' (interactive | worker | script)"),
                };
                let p = ailink_node::add_agent(home, &name, &role, project, &runner, mode)?;
                println!("✓ {}  {} (role {}, runner {})", p.agent_id, p.name, p.role, p.runner);
            }
            AgentCmd::Ls { json } => {
                let pf = ailink_node::load_profiles(home)?;
                if json {
                    println!("{}", serde_json::to_string(&pf.agents)?);
                } else {
                    for p in pf.agents {
                        println!("{:<28} {:<14} role={:<10} runner={:<12} project={}", p.agent_id, p.name, p.role, p.runner, p.project.unwrap_or_else(|| "-".into()));
                    }
                }
            }
        },
        Cmd::Status { json } => {
            let v = call(home, IpcOp::Status).await?;
            print_value(&v, json, |v| {
                format!(
                    "machine {} ({})  net {}\nserver: {}  seq {}  outbox {}  sessions {}",
                    v["alias"].as_str().unwrap_or("?"),
                    v["machineId"].as_str().unwrap_or("?"),
                    v["net"].as_str().unwrap_or("?"),
                    v["hub"].as_str().unwrap_or("?"),
                    v["seq"],
                    v["outbox"],
                    v["sessions"].as_array().map(|a| a.len()).unwrap_or(0)
                )
            });
        }
        Cmd::Ls { project, role, json } => {
            let v = call(home, IpcOp::ListAgents { project, role }).await?;
            print_value(&v, json, |v| {
                let mut s = String::new();
                for a in v["agents"].as_array().cloned().unwrap_or_default() {
                    s.push_str(&format!(
                        "{:<26} {:<12} {:<10} {:<24} {:<8}{}{}\n",
                        a["handle"].as_str().unwrap_or("?"),
                        a["runner"].as_str().unwrap_or("?"),
                        a["role"].as_str().unwrap_or("?"),
                        a["project"].as_str().unwrap_or("-"),
                        a["state"].as_str().unwrap_or("?"),
                        if a["sameMachine"] == true { "  (this PC)" } else { "" },
                        if a["stale"] == true { "  [stale]" } else { "" },
                    ));
                }
                s.trim_end().to_string()
            });
        }
        Cmd::Machines { json } => {
            let v = call(home, IpcOp::ListMachines).await?;
            print_value(&v, json, |v| {
                let mut s = String::new();
                for m in v["machines"].as_array().cloned().unwrap_or_default() {
                    let r = &m["reported"];
                    s.push_str(&format!(
                        "{:<16} {:<8} {} {} · {} · {} cores · {} GB · agents {}\n",
                        m["alias"].as_str().unwrap_or("?"),
                        m["state"].as_str().unwrap_or("?"),
                        r["os"].as_str().unwrap_or("?"),
                        r["osVersion"].as_str().unwrap_or(""),
                        r["cpu"].as_str().unwrap_or("-"),
                        r["cpuCores"],
                        r["ramGb"],
                        m["agentsOnline"]
                    ));
                }
                s.trim_end().to_string()
            });
        }
        Cmd::Attach { agent, takeover, json } => stream(home, Some(IpcOp::Attach { agent, runner: Some("cli".into()), pid: Some(std::process::id()), takeover }), json).await?,
        Cmd::Watch { as_agent, json } => stream(home, Some(IpcOp::Watch { agent: as_agent }), json).await?,
        Cmd::Send { as_agent, target, text, subject, intent, id, reply_to, stdin, json } => {
            let mut draft = if stdin || text.as_deref() == Some("-") {
                let mut buf = String::new();
                std::io::stdin().read_to_string(&mut buf)?;
                serde_json::from_str::<SendDraft>(&buf).context("stdin must be a JSON message draft")?
            } else {
                SendDraft { text, ..Default::default() }
            };
            if let Some(t) = target {
                draft.to = Some(Target::parse(&t).map_err(|e| anyhow!(e))?);
            }
            if draft.to.is_none() {
                bail!("missing target (e.g. @qa@studio-mbp, role:reviewer, project:shop-api)");
            }
            draft.subject = subject.or(draft.subject);
            draft.intent = intent.or(draft.intent);
            draft.id = id.or(draft.id);
            draft.in_reply_to = reply_to.or(draft.in_reply_to);
            let v = call(home, IpcOp::Send { as_agent: Some(as_agent), draft }).await?;
            print_value(&v, json, |v| {
                let remote = &v["remote"];
                let r = match remote["status"].as_str() {
                    Some("accepted") => format!("remote: {} accepted", remote["count"]),
                    Some("queued") => "remote: queued (server unreachable)".into(),
                    Some("rejected") => format!("remote: rejected — {} {}", remote["code"].as_str().unwrap_or(""), remote["msg"].as_str().unwrap_or("")),
                    _ => "remote: none".into(),
                };
                format!("✓ {}  local: {} delivered · {}{}", v["id"].as_str().unwrap_or("?"), v["local"], r, if v["duplicate"] == true { "  (duplicate)" } else { "" })
            });
        }
        Cmd::Inbox { as_agent, limit, json } => {
            let v = call(home, IpcOp::Inbox { agent: Some(as_agent), limit, message_id: None }).await?;
            print_value(&v, json, |v| {
                let mut s = String::new();
                for m in v["messages"].as_array().cloned().unwrap_or_default() {
                    let amf: Result<ailink_proto::amf::Amf, _> = serde_json::from_value(m.clone());
                    let text = amf.map(|a| a.render_text()).unwrap_or_default();
                    s.push_str(&format!(
                        "── {} from {} [{}]\n{}\n",
                        m["id"].as_str().unwrap_or("?"),
                        m["from"]["handle"].as_str().unwrap_or("?"),
                        m["intent"].as_str().unwrap_or("?"),
                        text.trim_end()
                    ));
                }
                if s.is_empty() { "(no messages)".into() } else { s.trim_end().to_string() }
            });
        }
        Cmd::Ack { as_agent, message_id } => {
            call(home, IpcOp::Ack { agent: Some(as_agent), message_id: message_id.clone() }).await?;
            println!("✓ acked {message_id}");
        }
        Cmd::Wait { as_agent, timeout, json } => {
            let v = call(home, IpcOp::Wait { agent: Some(as_agent), timeout_ms: timeout * 1000 }).await?;
            print_value(&v, json, |v| match v["message"].as_object() {
                Some(m) => format!("{} from {}: {}", m["id"].as_str().unwrap_or("?"), m["from"]["handle"].as_str().unwrap_or("?"), m["subject"].as_str().unwrap_or("")),
                None => "(timed out)".into(),
            });
        }
    }
    Ok(())
}

fn print_value(v: &Value, json: bool, human: impl Fn(&Value) -> String) {
    if json {
        println!("{v}");
    } else {
        println!("{}", human(v));
    }
}

async fn connect(home: &Path) -> Result<Stream> {
    let name = ailink_node::ipc_name(home)?;
    Stream::connect(name).await.with_context(|| format!("no node running for {} (start it with `ailink node run`)", home.display()))
}

/// One request, one response (pushes are ignored).
async fn call(home: &Path, op: IpcOp) -> Result<Value> {
    let conn = connect(home).await?;
    let mut rd = BufReader::new(&conn);
    let mut wr = &conn;
    let mut line = serde_json::to_vec(&IpcRequest { id: 1, op })?;
    line.push(b'\n');
    wr.write_all(&line).await?;
    let mut buf = String::new();
    loop {
        buf.clear();
        if rd.read_line(&mut buf).await? == 0 {
            bail!("node closed the connection");
        }
        match serde_json::from_str::<IpcOut>(buf.trim())? {
            IpcOut::Response(r) if r.id == 1 => {
                return if r.ok {
                    Ok(r.result.unwrap_or(Value::Null))
                } else {
                    let e = r.error.unwrap_or(ailink_proto::ipc::IpcError { code: "ERROR".into(), msg: "unknown".into() });
                    Err(anyhow!("{}: {}", e.code, e.msg))
                };
            }
            _ => continue,
        }
    }
}

/// Long-lived connection: send `first`, then print every response/push as a JSON line
/// (or a human line) until interrupted. Ctrl-C detaches cleanly.
async fn stream(home: &Path, first: Option<IpcOp>, json_out: bool) -> Result<()> {
    let conn = connect(home).await?;
    let mut rd = BufReader::new(&conn);
    let mut wr = &conn;
    let attaching = matches!(first, Some(IpcOp::Attach { .. }));
    if let Some(op) = first {
        let mut line = serde_json::to_vec(&IpcRequest { id: 1, op })?;
        line.push(b'\n');
        wr.write_all(&line).await?;
    }
    let mut buf = String::new();
    loop {
        buf.clear();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                if attaching {
                    let mut line = serde_json::to_vec(&IpcRequest { id: 2, op: IpcOp::Detach })?;
                    line.push(b'\n');
                    let _ = wr.write_all(&line).await;
                    let _ = tokio::time::timeout(Duration::from_millis(500), rd.read_line(&mut buf)).await;
                }
                return Ok(());
            }
            n = rd.read_line(&mut buf) => {
                if n? == 0 { return Ok(()); }
                let out: IpcOut = serde_json::from_str(buf.trim())?;
                match out {
                    IpcOut::Response(r) if r.id == 1 => {
                        if !r.ok {
                            let e = r.error.map(|e| format!("{}: {}", e.code, e.msg)).unwrap_or_default();
                            if json_out { println!("{}", json!({"event": "error", "data": e})); }
                            bail!(e);
                        }
                        let ev = if attaching { "attached" } else { "watching" };
                        if json_out {
                            println!("{}", json!({"event": ev, "data": r.result}));
                        } else {
                            let d = r.result.unwrap_or_default();
                            println!("● {ev} {} — server {}", d["agent"]["handle"].as_str().unwrap_or(""), d["hub"].as_str().unwrap_or("?"));
                        }
                    }
                    IpcOut::Response(_) => {}
                    IpcOut::Push(p) => {
                        if json_out {
                            println!("{}", serde_json::to_string(&p)?);
                        } else {
                            match p.event.as_str() {
                                "message" => println!("✉ {} → {}: {}", p.data["message"]["from"]["handle"].as_str().unwrap_or("?"), p.data["recipientHandle"].as_str().unwrap_or("?"), p.data["message"]["subject"].as_str().or(p.data["message"]["parts"][0]["text"].as_str()).unwrap_or("")),
                                "roster" => println!("◆ {} {}", p.data["kind"].as_str().unwrap_or("?"), p.data["agent"]["handle"].as_str().or(p.data["machine"]["alias"].as_str()).unwrap_or("")),
                                other => println!("· {other} {}", p.data),
                            }
                        }
                    }
                }
                use std::io::Write;
                let _ = std::io::stdout().flush();
            }
        }
    }
}
