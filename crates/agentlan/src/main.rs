//! `agentlan` — one binary: hub, node daemon and CLI client.

mod hooks;
mod wakecli;

use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use agentlan_proto::amf::{SendDraft, Target};
use agentlan_proto::ipc::{IpcOp, IpcOut, IpcRequest};
use agentlan_proto::join::JoinCode;
use agentlan_proto::records::AgentMode;
use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use interprocess::local_socket::tokio::{Stream, prelude::*};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Parser)]
#[command(name = "agentlan", version, about = "Link AI agents across PCs on your LAN")]
struct Cli {
    /// Data directory (default: per-user app data; env AGENTLAN_HOME).
    #[arg(long, global = true, env = "AGENTLAN_HOME")]
    home: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// One command for the server PC: sets everything up on first run, starts the server
    /// and this PC's node, and prints the `agentlan connect …` line for other PCs.
    Up {
        #[arg(long, default_value = "home-lab")]
        name: String,
        /// Address to listen on (default: the saved setting, initially 0.0.0.0:47820).
        #[arg(long)]
        listen: Option<String>,
        /// Address(es) other PCs should use (default: LAN, then Tailscale). Repeatable.
        #[arg(long)]
        advertise: Vec<String>,
        /// Advertise only this PC's Tailscale name/IPs.
        #[arg(long, conflicts_with = "advertise")]
        tailscale: bool,
        /// This PC's name on the network (default: hostname).
        #[arg(long)]
        alias: Option<String>,
    },
    /// One command for every other PC: joins on first run (needs the join code), then runs the node.
    Connect {
        code: Option<String>,
        /// This PC's name on the network (default: hostname).
        #[arg(long)]
        alias: Option<String>,
    },
    /// Server (hub) commands — run them on whichever PC you pick as the server.
    Hub {
        #[command(subcommand)]
        cmd: HubCmd,
    },
    /// Join this PC to a network using a join code from `agentlan hub invite`.
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
        /// Continue an existing thread (thr_…).
        #[arg(long)]
        thread: Option<String>,
        #[arg(long)]
        stdin: bool,
        #[arg(long)]
        json: bool,
    },
    /// Ask one agent and wait for its reply. TARGET: @name@machine | agt_… | role:R | project:P (one is picked).
    Request {
        #[arg(long = "as")]
        as_agent: String,
        target: String,
        text: String,
        #[arg(long)]
        subject: Option<String>,
        /// Seconds to wait for the reply.
        #[arg(long, default_value_t = 30)]
        wait: u64,
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
    /// MCP stdio server for an AI agent (Claude Code, Codex, …). Stdout is the MCP channel.
    Mcp {
        /// Start as an existing agent profile (or `auto`: match the git remote of the working
        /// dir). Usually omitted: the agent calls `connect` and `register` itself.
        #[arg(long)]
        agent: Option<String>,
        /// claude-code | codex | generic — message format (default: detected from the MCP client).
        #[arg(long)]
        runner: Option<String>,
        /// Messaging tools only, for the --agent identity (used for woken workers).
        #[arg(long)]
        worker: bool,
    },
    /// Run a runner hook (reads the hook JSON on stdin). Example: `hook claude post-tool-use --agent backend`.
    Hook {
        /// claude
        runner: String,
        /// session-start | user-prompt-submit | post-tool-use | stop
        event: String,
        #[arg(long, default_value = "auto")]
        agent: String,
    },
    /// Loop-guard threads.
    Thread {
        #[command(subcommand)]
        cmd: ThreadCmd,
    },
    /// Worker wake: start an idle worker agent (through ACP) when mail arrives for it.
    Wake {
        #[command(subcommand)]
        cmd: WakeCmd,
    },
    /// Development helpers (used by tests).
    #[command(hide = true)]
    Dev {
        #[command(subcommand)]
        cmd: DevCmd,
    },
    /// Hook configuration helpers.
    Hooks {
        #[command(subcommand)]
        cmd: HooksCmd,
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
        /// Address(es) other PCs should use to reach this server
        /// (default: this PC's LAN addresses, then its Tailscale name/IPs; virtual adapters skipped).
        #[arg(long)]
        addr: Vec<String>,
        /// Only advertise this PC's Tailscale MagicDNS name and tailnet IPs.
        #[arg(long, conflicts_with = "addr")]
        tailscale: bool,
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
enum WakeCmd {
    /// Enable wake for a worker profile (`agentlan agent add <name> --mode worker`).
    Enable {
        name: String,
        /// codex (codex-acp) | claude (claude-agent-acp) | custom (any ACP agent, with --command)
        #[arg(long)]
        agent: String,
        /// The ACP agent command (default: the adapter found on PATH, pinned as an absolute path).
        #[arg(long)]
        command: Option<String>,
        /// Working directory (project) for the agent (default: the current directory).
        #[arg(long)]
        cwd: Option<PathBuf>,
        /// messages (default): AgentLAN tools only. work: reads/edits inside --cwd (Codex: commands in
        /// its sandbox). full: approve everything, unattended.
        #[arg(long, default_value = "messages")]
        access: String,
        /// Max seconds per wake.
        #[arg(long, default_value_t = 600)]
        timeout: u64,
        /// Extra environment for the agent, e.g. --env CLAUDE_CONFIG_DIR=C:\Users\me\.claude-work
        /// (repeatable). Stored with the worker in this PC's private data dir.
        #[arg(long = "env", value_name = "KEY=VALUE")]
        env: Vec<String>,
        /// Claude Code only: run your own Claude CLI (sets CLAUDE_CODE_EXECUTABLE). It must take
        /// Claude's normal arguments. On Windows it must be an .exe (scripts can't be started
        /// without a shell); for a wrapper that only sets variables, use --env instead.
        #[arg(long, value_name = "PATH")]
        claude_cli: Option<PathBuf>,
    },
    /// Disable wake for a worker.
    Disable { name: String },
    /// Show workers with wake enabled and their last wake.
    Ls {
        #[arg(long)]
        json: bool,
    },
    /// Wake a worker now (it must have unread mail).
    Now { name: String },
}

#[derive(Subcommand)]
enum DevCmd {
    /// A minimal fake ACP agent for tests.
    FakeAcp {
        #[arg(long)]
        log: PathBuf,
    },
    /// Live delivery into a Codex thread (the same path the MCP adapter uses).
    CodexInject {
        #[arg(long)]
        thread: String,
        #[arg(long)]
        text: String,
    },
}

#[derive(Subcommand)]
enum ThreadCmd {
    /// List threads paused by the loop guard.
    Ls,
    /// Resume a paused thread (thr_…) or pair (pair:…). Must be run by a human at an interactive terminal.
    Resume { thread_id: String },
}

#[derive(Subcommand)]
enum HooksCmd {
    /// Print the hook settings snippet for a runner (does not write any file).
    Print {
        /// claude
        runner: String,
        /// A pre-made profile, or `auto` (default): the identity this Claude session
        /// registered through the MCP tools, else the profile matching the repository.
        #[arg(long, default_value = "auto")]
        agent: String,
    },
}

#[derive(Subcommand)]
enum NodeCmd {
    /// Run the node daemon.
    Run,
    /// Stop the node daemon running for this home (e.g. one started by an agent's `connect`).
    Stop,
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
    directories::ProjectDirs::from("dev", "agentlan", "agentlan").map(|d| d.data_dir().to_path_buf()).unwrap_or_else(|| PathBuf::from(".agentlan"))
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::try_from_env("AGENTLAN_LOG").unwrap_or_else(|_| "warn".into())).with_writer(std::io::stderr).init();
    let home = cli.home.clone().unwrap_or_else(default_home);
    let home_explicit = cli.home.is_some();
    if let Cmd::Hooks { cmd: HooksCmd::Print { runner, agent } } = &cli.cmd {
        if let Err(e) = hooks::print(&home, runner, agent, home_explicit) {
            eprintln!("error: {e:#}");
            std::process::exit(1);
        }
        return;
    }
    if let Err(e) = run(cli.cmd, &home).await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

async fn run(cmd: Cmd, home: &Path) -> Result<()> {
    match cmd {
        Cmd::Hub { cmd } => match cmd {
            HubCmd::Init { name, listen } => {
                let info = agentlan_hub::init(home, &name, &listen)?;
                println!("✓ server created in {}\n  key fingerprint {}\n  listens on {}", agentlan_hub::hub_dir(home).display(), info.fingerprint, info.listen);
                println!("next: `agentlan hub run`, then `agentlan hub invite` for each PC");
            }
            HubCmd::Invite { addr, tailscale, ttl_min } => {
                let addrs = if addr.is_empty() {
                    let hf = agentlan_hub::load(home)?;
                    let port: u16 = hf.listen.rsplit(':').next().and_then(|p| p.parse().ok()).unwrap_or(agentlan_hub::DEFAULT_PORT);
                    let scope = if tailscale { agentlan_node::netif::InviteScope::Tailscale } else { agentlan_node::netif::InviteScope::Default };
                    let a = agentlan_node::netif::invite_addrs(port, scope);
                    if a.is_empty() {
                        if tailscale {
                            bail!("Tailscale is not running on this PC (or its CLI was not found); start it, or pass --addr HOST:PORT");
                        }
                        bail!("no usable LAN or Tailscale address found; pass --addr HOST:PORT");
                    }
                    a
                } else {
                    addr
                };
                // stdout stays just the code (scriptable); the address list goes to stderr.
                eprintln!("join code advertises: {}", addrs.join(", "));
                let code = agentlan_hub::invite(home, addrs, Duration::from_secs(ttl_min * 60))?;
                println!("{}", code.encode());
            }
            HubCmd::Run { listen } => agentlan_hub::run(home, listen).await?,
        },
        Cmd::Up { name, listen, advertise, tailscale, alias } => {
            // The node may already be running (e.g. started by an agent's `connect`): then only
            // the server starts here and the node is left as it is.
            let node_up = node_running(home).await;
            let fresh = !agentlan_hub::hub_dir(home).join("hub.json").exists();
            let listen_override = if fresh {
                let l = listen.clone().unwrap_or_else(|| format!("0.0.0.0:{}", agentlan_hub::DEFAULT_PORT));
                let info = agentlan_hub::init(home, &name, &l)?;
                eprintln!("✓ created server '{name}' (key {})", info.fingerprint);
                None
            } else {
                let hf = agentlan_hub::load(home)?;
                match &listen {
                    Some(l) if *l != hf.listen => {
                        eprintln!("(server '{}' is configured for {}; using --listen {l} for this run)", hf.net, hf.listen);
                        Some(l.clone())
                    }
                    _ => None,
                }
            };
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let hub_home = home.to_path_buf();
            let mut hub_task = tokio::spawn(async move { agentlan_hub::run_with_ready(&hub_home, listen_override, Some(ready_tx)).await });
            // Wait for the server to report the address it actually bound (or fail fast).
            let bound = tokio::select! {
                r = ready_rx => r.map_err(|_| anyhow!("the server stopped during startup"))?,
                r = &mut hub_task => {
                    return match r { Ok(Err(e)) => Err(e), _ => Err(anyhow!("the server stopped during startup")) };
                }
                _ = tokio::time::sleep(Duration::from_secs(15)) => bail!("the server did not start listening"),
            };
            let port = bound.port();
            // This PC's own node reaches the server at the bound address (loopback if it
            // listens on all interfaces).
            let local_addr = if bound.ip().is_unspecified() {
                if bound.is_ipv6() { format!("[::1]:{port}") } else { format!("127.0.0.1:{port}") }
            } else {
                bound.to_string()
            };
            match agentlan_node::load_config(home) {
                Err(_) => {
                    let code = agentlan_hub::invite(home, vec![local_addr.clone()], Duration::from_secs(300))?;
                    let alias = alias.unwrap_or_else(|| agentlan_node::machine_info(false).hostname);
                    let (_, alias) = agentlan_node::join(home, &code, &alias).await?;
                    eprintln!("✓ this PC joined as '{alias}'");
                }
                Ok(cfg) => {
                    if let Some(a) = alias.filter(|a| *a != cfg.alias) {
                        eprintln!("(this PC already joined as '{}'; --alias {a} ignored — aliases are fixed at join)", cfg.alias);
                    }
                }
            }
            let addrs = if advertise.is_empty() {
                let scope = if tailscale { agentlan_node::netif::InviteScope::Tailscale } else { agentlan_node::netif::InviteScope::Default };
                agentlan_node::netif::invite_addrs(port, scope)
            } else {
                advertise
            };
            if addrs.is_empty() {
                bail!("no LAN or Tailscale address found for other PCs; pass --advertise HOST:PORT");
            }
            let code = agentlan_hub::invite(home, addrs.clone(), Duration::from_secs(15 * 60))?;
            println!("✓ AgentLAN server listening on {bound}; other PCs reach it at: {}", addrs.join(", "));
            println!("\nOn each other PC, run (code valid 15 min; `agentlan hub invite` makes more):\n\n  agentlan connect {}\n", code.encode());
            println!("Agents connect from their session through the agentlan MCP tools (`connect`, then `register`).");
            if node_up {
                println!("(this PC's node was already running and keeps running)");
                hub_task.await??;
            } else {
                let node_home = home.to_path_buf();
                let node_task = tokio::spawn(async move { agentlan_node::run(&node_home).await });
                tokio::select! {
                    r = &mut hub_task => r??,
                    r = node_task => {
                        r??;
                        // `agentlan node stop` stopped only the node; keep serving the others.
                        println!("(this PC's node stopped; the server keeps running)");
                        hub_task.await??;
                    }
                }
            }
        }
        Cmd::Connect { code, alias } => {
            if node_running(home).await {
                let cfg = agentlan_node::load_config(home)?;
                println!("✓ this PC's node is already running (network '{}' as {})", cfg.net, cfg.alias);
                return Ok(());
            }
            match agentlan_node::load_config(home) {
                Ok(cfg) => {
                    if code.is_some() {
                        eprintln!("(this PC already joined '{}' as {}; ignoring the code)", cfg.net, cfg.alias);
                    }
                    if let Some(a) = alias.filter(|a| *a != cfg.alias) {
                        eprintln!("(--alias {a} ignored — this PC joined as '{}'; aliases are fixed at join)", cfg.alias);
                    }
                }
                Err(_) => {
                    let Some(code) = code else {
                        bail!("this PC hasn't joined yet: run `agentlan connect <join code>` (the server PC prints one with `agentlan up` or `agentlan hub invite`)");
                    };
                    let code = JoinCode::decode(&code).map_err(|e| anyhow!(e))?;
                    let alias = alias.unwrap_or_else(|| agentlan_node::machine_info(false).hostname);
                    let (_, alias) = agentlan_node::join(home, &code, &alias).await?;
                    println!("✓ joined network '{}' as {alias}", code.net);
                }
            }
            eprintln!("Starting the node… agents on this PC connect through the agentlan MCP tools (`connect`, then `register`).");
            agentlan_node::run(home).await?;
        }
        Cmd::Join { code, alias } => {
            let code = JoinCode::decode(&code).map_err(|e| anyhow!(e))?;
            let alias = alias.unwrap_or_else(|| agentlan_node::machine_info(false).hostname);
            let (mid, alias) = agentlan_node::join(home, &code, &alias).await?;
            println!("✓ joined network '{}' as {alias} ({mid})\nnext: `agentlan agent add …`, then `agentlan node run`", code.net);
        }
        Cmd::Node { cmd: NodeCmd::Run } => agentlan_node::run(home).await?,
        Cmd::Node { cmd: NodeCmd::Stop } => {
            if !agentlan_node::is_running(home).await {
                println!("no AgentLAN node is running for {}", home.display());
                return Ok(());
            }
            call(home, IpcOp::Shutdown).await?;
            for _ in 0..50 {
                if !agentlan_node::is_running(home).await {
                    println!("✓ node stopped");
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            bail!("the node did not stop");
        }
        Cmd::Agent { cmd } => match cmd {
            AgentCmd::Add { name, role, project, runner, mode } => {
                let mode = match mode.as_str() {
                    "interactive" => AgentMode::Interactive,
                    "worker" => AgentMode::Worker,
                    "script" => AgentMode::Script,
                    m => bail!("unknown mode '{m}' (interactive | worker | script)"),
                };
                let p = agentlan_node::add_agent(home, &name, &role, project, &runner, mode)?;
                println!("✓ {}  {} (role {}, runner {})", p.agent_id, p.name, p.role, p.runner);
            }
            AgentCmd::Ls { json } => {
                let pf = agentlan_node::load_profiles(home)?;
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
        Cmd::Attach { agent, takeover, json } => {
            stream(home, Some(IpcOp::Attach { agent, runner: Some("cli".into()), pid: Some(std::process::id()), host_pid: None, takeover }), json).await?
        }
        Cmd::Watch { as_agent, json } => stream(home, Some(IpcOp::Watch { agent: as_agent }), json).await?,
        Cmd::Send { as_agent, target, text, subject, intent, id, reply_to, thread, stdin, json } => {
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
            draft.thread_id = thread.or(draft.thread_id);
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
        Cmd::Request { as_agent, target, text, subject, wait, json } => {
            use agentlan_proto::amf::Pick;
            let to = match Target::parse(&target).map_err(|e| anyhow!(e))? {
                Target::Role { role, project, .. } => Target::Role { role, project, pick: Pick::One },
                Target::Project { project, .. } => Target::Project { project, pick: Pick::One },
                t @ (Target::Agent { .. } | Target::Handle { .. }) => t,
                _ => bail!("request needs a single recipient: @name@machine, agt_…, role:R or project:P"),
            };
            let draft = SendDraft {
                to: Some(to),
                intent: Some("request".into()),
                subject,
                text: Some(text),
                reply_by: Some(agentlan_proto::now() + chrono::Duration::seconds(600)),
                ..Default::default()
            };
            // One node operation: send + wait for the actual recipient's reply.
            let mut out = call(home, IpcOp::Request { as_agent: Some(as_agent.clone()), draft, timeout_ms: wait * 1000 }).await?;
            let id = out["sent"]["id"].as_str().unwrap_or_default().to_string();
            out["acked"] = json!(ack_answer(home, &as_agent, &out["reply"]).await);
            print_value(&out, json, |o| {
                // Index the Value (missing keys are Null), not the Map (missing keys panic).
                let m = &o["reply"];
                if m.is_object() {
                    format!("↩ reply from {}: {}", m["from"]["handle"].as_str().unwrap_or("?"), m["parts"][0]["text"].as_str().or(m["subject"].as_str()).unwrap_or(""))
                } else {
                    format!("sent {id}; no reply within {wait}s (it will land in the inbox)")
                }
            });
        }
        Cmd::Inbox { as_agent, limit, json } => {
            let v = call(home, IpcOp::Inbox { agent: Some(as_agent), limit, message_id: None }).await?;
            print_value(&v, json, |v| {
                let mut s = String::new();
                for m in v["messages"].as_array().cloned().unwrap_or_default() {
                    let amf: Result<agentlan_proto::amf::Amf, _> = serde_json::from_value(m.clone());
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
        Cmd::Mcp { agent, runner, worker } => {
            agentlan_mcp::serve(home.to_path_buf(), agent, runner.as_deref().map(agentlan_mcp::Runner::parse), worker).await?;
        }
        Cmd::Hook { runner, event, agent } => {
            if runner != "claude" && runner != "claude-code" {
                bail!("only Claude Code hooks are supported (runner 'claude')");
            }
            hooks::run(home, &event, &agent).await?;
        }
        Cmd::Hooks { .. } => unreachable!("handled before dispatch"),
        Cmd::Wake { cmd } => match cmd {
            WakeCmd::Enable { name, agent, command, cwd, access, timeout, env, claude_cli } => {
                wakecli::enable(home, &name, &agent, command, cwd, &access, timeout, env, claude_cli)?
            }
            WakeCmd::Disable { name } => wakecli::disable(home, &name)?,
            WakeCmd::Ls { json } => wakecli::ls(home, json).await?,
            WakeCmd::Now { name } => wakecli::now(home, &name).await?,
        },
        Cmd::Dev { cmd: DevCmd::FakeAcp { log } } => wakecli::fake_acp(&log)?,
        Cmd::Dev { cmd: DevCmd::CodexInject { thread, text } } => {
            let how = agentlan_mcp::live::codex_inject(&thread, &text).await?;
            println!("delivered via {how}");
        }
        Cmd::Thread { cmd: ThreadCmd::Ls } => {
            let v = call(home, IpcOp::Status).await?;
            let p = v["pausedThreads"].as_array().cloned().unwrap_or_default();
            if p.is_empty() {
                println!("(no paused threads)");
            }
            for t in p {
                println!("{}  paused {}  — {}", t["key"].as_str().unwrap_or("?"), t["pausedAt"].as_str().unwrap_or("?"), t["reason"].as_str().unwrap_or(""));
            }
        }
        Cmd::Thread { cmd: ThreadCmd::Resume { thread_id } } => {
            use std::io::{BufRead, IsTerminal, Write};
            // An agent can run this CLI too; requiring a terminal keeps resuming a human act.
            if !std::io::stdin().is_terminal() {
                bail!("`thread resume` must be run by a human at an interactive terminal");
            }
            print!("Resume thread {thread_id}? Agents in it may exchange automated messages again. [y/N] ");
            std::io::stdout().flush()?;
            let mut ans = String::new();
            std::io::stdin().lock().read_line(&mut ans)?;
            if !matches!(ans.trim(), "y" | "Y" | "yes") {
                bail!("not resumed");
            }
            let v = call(home, IpcOp::ResumeThread { thread_id: thread_id.clone() }).await?;
            println!("{}", if v["resumed"] == true { format!("✓ resumed {}", v["key"].as_str().unwrap_or(&thread_id)) } else { format!("{thread_id} was not paused") });
        }
        Cmd::Wait { as_agent, timeout, json } => {
            let mut v = call(home, IpcOp::Wait { agent: Some(as_agent.clone()), timeout_ms: timeout * 1000, in_reply_to: None }).await?;
            v["acked"] = json!(ack_answer(home, &as_agent, &v["message"]).await);
            print_value(&v, json, |v| {
                let m = &v["message"]; // Value indexing: a missing subject is Null, not a panic
                if m.is_object() {
                    let what = m["subject"].as_str().or(m["parts"][0]["text"].as_str()).unwrap_or("");
                    format!("{} from {}: {what}", m["id"].as_str().unwrap_or("?"), m["from"]["handle"].as_str().unwrap_or("?"))
                } else {
                    "(timed out)".into()
                }
            });
        }
    }
    Ok(())
}

/// Mark a returned message handled if it's an answer (see `amf::is_answer`), as the MCP
/// `request` and `wait_for_message` tools do.
async fn ack_answer(home: &Path, as_agent: &str, m: &Value) -> bool {
    let Some(mid) = m["id"].as_str().filter(|_| agentlan_proto::amf::is_answer(m)) else { return false };
    call(home, IpcOp::Ack { agent: Some(as_agent.to_string()), message_id: mid.to_string() }).await.is_ok()
}

/// Is a node already serving this home's IPC endpoint?
async fn node_running(home: &Path) -> bool {
    match agentlan_node::ipc_name(home) {
        Ok(name) => Stream::connect(name).await.is_ok(),
        Err(_) => false,
    }
}

fn print_value(v: &Value, json: bool, human: impl Fn(&Value) -> String) {
    if json {
        println!("{v}");
    } else {
        println!("{}", human(v));
    }
}

async fn connect(home: &Path) -> Result<Stream> {
    let name = agentlan_node::ipc_name(home)?;
    Stream::connect(name).await.with_context(|| format!("no node running for {} (start it with `agentlan node run`)", home.display()))
}

/// One request, one response (pushes are ignored).
pub(crate) async fn call(home: &Path, op: IpcOp) -> Result<Value> {
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
                    let e = r.error.unwrap_or(agentlan_proto::ipc::IpcError { code: "ERROR".into(), msg: "unknown".into() });
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
