#![cfg(unix)]
//! End-to-end tests that drive the real `agentlan` binary as separate processes
//! on 127.0.0.1 (Milestone 1 exit criteria).

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::Value;

const BIN: &str = env!("CARGO_BIN_EXE_agentlan");
static N: AtomicU32 = AtomicU32::new(0);

struct Env {
    root: PathBuf,
    port: u16,
    kids: Vec<Child>,
    extra_env: Vec<(&'static str, String)>,
}

impl Env {
    fn new() -> Env {
        // Short path: Unix socket paths are limited to ~104 bytes on macOS.
        let root = PathBuf::from(format!("/tmp/agl-{}-{}", std::process::id(), N.fetch_add(1, Ordering::SeqCst)));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        Env { root, port, kids: vec![], extra_env: vec![] }
    }

    fn home(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    fn cmd(&self, home: &str, args: &[&str]) -> Command {
        let mut c = Command::new(BIN);
        c.arg("--home").arg(self.home(home)).args(args);
        c.env("AGENTLAN_HEARTBEAT_MS", "300").env("AGENTLAN_LEASE_MS", "2000").env("AGENTLAN_BACKOFF_MAX_MS", "400").env("AGENTLAN_LOG", "warn");
        for (k, v) in &self.extra_env {
            c.env(k, v);
        }
        c
    }

    fn run(&self, home: &str, args: &[&str]) -> Output {
        let out = self.cmd(home, args).output().unwrap();
        assert!(out.status.success(), "agentlan {args:?} failed: {}", String::from_utf8_lossy(&out.stderr));
        out
    }

    fn json(&self, home: &str, args: &[&str]) -> Value {
        let out = self.run(home, args);
        serde_json::from_slice(&out.stdout).unwrap_or_else(|e| panic!("bad json from {args:?}: {e}: {}", String::from_utf8_lossy(&out.stdout)))
    }

    fn spawn(&mut self, home: &str, args: &[&str]) -> usize {
        let log = std::fs::File::create(self.root.join(format!("{home}-{}.log", self.kids.len()))).unwrap();
        let child = self.cmd(home, args).stdout(Stdio::null()).stderr(log).spawn().unwrap();
        self.kids.push(child);
        self.kids.len() - 1
    }

    /// Spawn a streaming command and return a channel of its stdout JSON lines.
    fn stream(&mut self, home: &str, args: &[&str]) -> mpsc::Receiver<Value> {
        let mut child = self.cmd(home, args).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
        let out = child.stdout.take().unwrap();
        self.kids.push(child);
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(out).lines().map_while(Result::ok) {
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    if tx.send(v).is_err() {
                        break;
                    }
                }
            }
        });
        rx
    }

    fn kill(&mut self, idx: usize) {
        let _ = self.kids[idx].kill();
        let _ = self.kids[idx].wait();
    }

    fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// hub init + run, and join each node. Returns the hub child index.
    fn network(&mut self, nodes: &[(&str, &str)]) -> usize {
        self.run("hub", &["hub", "init", "--name", "e2e", "--listen", &self.addr()]);
        let codes: Vec<String> = nodes
            .iter()
            .map(|_| String::from_utf8(self.run("hub", &["hub", "invite", "--addr", &self.addr()]).stdout).unwrap().trim().to_string())
            .collect();
        let hub = self.spawn("hub", &["hub", "run"]);
        wait_port(self.port);
        for ((home, alias), code) in nodes.iter().zip(codes) {
            self.run(home, &["join", &code, "--alias", alias]);
        }
        hub
    }

    fn start_node(&mut self, home: &str) -> usize {
        let idx = self.spawn(home, &["node", "run"]);
        let h = self.home(home);
        wait_until("node IPC up", Duration::from_secs(10), || self.cmd_ok(&h));
        idx
    }

    fn cmd_ok(&self, home: &Path) -> bool {
        Command::new(BIN).arg("--home").arg(home).args(["status", "--json"]).output().map(|o| o.status.success()).unwrap_or(false)
    }

    fn status(&self, home: &str) -> Value {
        self.json(home, &["status", "--json"])
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        for k in &mut self.kids {
            let _ = k.kill();
            let _ = k.wait();
        }
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.root);
        } else {
            eprintln!("test data kept in {}", self.root.display());
        }
    }
}

fn wait_port(port: u16) {
    wait_until("hub port open", Duration::from_secs(10), || std::net::TcpStream::connect(("127.0.0.1", port)).is_ok());
}

fn wait_until(what: &str, max: Duration, mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while t.elapsed() < max {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out waiting for {what}");
}

fn expect(rx: &mpsc::Receiver<Value>, what: &str, max: Duration, pred: impl Fn(&Value) -> bool) -> Value {
    let t = Instant::now();
    while let Some(left) = max.checked_sub(t.elapsed()) {
        match rx.recv_timeout(left) {
            Ok(v) if pred(&v) => return v,
            Ok(_) => continue,
            Err(_) => break,
        }
    }
    panic!("timed out waiting for {what}");
}

const T: Duration = Duration::from_secs(10);

/// Criteria 1, 2 and 4: hub + two nodes as separate processes; attach → roster for the
/// newcomer and a join event on the other node; cross-node delivery; retry deduplicated.
#[test]
fn cross_machine_announce_delivery_and_dedup() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend", "--runner", "claude-code", "--project", "shop"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer", "--runner", "codex", "--project", "shop"]);
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");

    let rev = e.stream("n2", &["attach", "reviewer", "--json"]);
    expect(&rev, "reviewer attached", T, |v| v["event"] == "attached");
    wait_until("reviewer visible on n1", T, || {
        e.json("n1", &["ls", "--json"])["agents"].as_array().unwrap().iter().any(|a| a["handle"] == "reviewer@pc-two" && a["state"] == "online")
    });

    // Newcomer gets the roster; the other node hears the join.
    let back = e.stream("n1", &["attach", "backend", "--json"]);
    let att = expect(&back, "backend attached", T, |v| v["event"] == "attached");
    let roster = att["data"]["roster"].as_array().unwrap();
    assert!(roster.iter().any(|a| a["handle"] == "reviewer@pc-two" && a["state"] == "online" && a["sameMachine"] == false), "roster: {roster:?}");
    expect(&rev, "join event for backend on n2", T, |v| v["event"] == "roster" && v["data"]["kind"] == "joined" && v["data"]["agent"]["handle"] == "backend@pc-one");

    // Cross-node message.
    let r1 = e.json("n1", &["send", "--as", "backend", "@reviewer@pc-two", "please review", "--subject", "PR 12", "--id", "msg_e2e_dedup", "--json"]);
    assert_eq!(r1["remote"]["status"], "accepted", "{r1}");
    assert_eq!(r1["remote"]["count"], 1);
    assert_eq!(r1["local"], 0);
    let wire_id = r1["id"].as_str().unwrap().to_string();
    assert_eq!(r1["clientId"], "msg_e2e_dedup");
    let m = expect(&rev, "message pushed to reviewer", T, |v| v["event"] == "message");
    assert_eq!(m["data"]["message"]["id"], wire_id.as_str());
    assert_eq!(m["data"]["message"]["from"]["handle"], "backend@pc-one");

    // Retry with the same id: deduplicated, delivered once.
    let r2 = e.json("n1", &["send", "--as", "backend", "@reviewer@pc-two", "please review", "--subject", "PR 12", "--id", "msg_e2e_dedup", "--json"]);
    assert_eq!(r2["duplicate"], true, "{r2}");
    std::thread::sleep(Duration::from_millis(500));
    let inbox = e.json("n2", &["inbox", "--as", "reviewer", "--json"]);
    let ids: Vec<&str> = inbox["messages"].as_array().unwrap().iter().map(|m| m["id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec![wire_id.as_str()], "{inbox}");

    // The same retry key used by a *different* sender is a different message, not a duplicate.
    let r3 = e.json("n2", &["send", "--as", "reviewer", "@backend@pc-one", "lgtm", "--id", "msg_e2e_dedup", "--json"]);
    assert_eq!(r3["duplicate"], false, "{r3}");
    assert_eq!(r3["remote"]["status"], "accepted", "{r3}");
    assert_ne!(r3["id"].as_str().unwrap(), wire_id);
    let m = expect(&back, "reply pushed to backend", T, |v| v["event"] == "message");
    assert_eq!(m["data"]["message"]["parts"][0]["text"], "lgtm");
}

/// A second daemon on the same home is refused and does not disturb the running one;
/// oversized messages are rejected before anything is committed.
#[test]
fn single_instance_locks_and_size_limits() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend"]);
    e.run("n1", &["agent", "add", "qa", "--role", "qa"]);
    e.start_node("n1");
    wait_until("node ready", T, || e.status("n1")["hub"] == "ready");

    let second_node = e.cmd("n1", &["node", "run"]).output().unwrap();
    assert!(!second_node.status.success());
    assert!(String::from_utf8_lossy(&second_node.stderr).contains("already using this directory"));
    let second_hub = e.cmd("hub", &["hub", "run"]).output().unwrap();
    assert!(!second_hub.status.success());
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(e.status("n1")["hub"], "ready", "the running hub/node must be unaffected");

    // Via stdin: Linux caps a single argv entry at 128 KiB.
    let big = serde_json::json!({"to": {"kind": "handle", "handle": "qa@pc-one"}, "text": "x".repeat(300 * 1024)}).to_string();
    let mut child = e.cmd("n1", &["send", "--as", "backend", "--stdin"]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    {
        use std::io::Write;
        child.stdin.take().unwrap().write_all(big.as_bytes()).unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("TOO_LARGE"), "{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(e.status("n1")["outbox"], 0);
}

/// Criterion 3: a same-machine message is delivered while no hub process is running
/// for the node's whole lifetime.
#[test]
fn same_machine_delivery_without_hub() {
    let mut e = Env::new();
    let hub = e.network(&[("n1", "pc-one")]);
    e.kill(hub); // the node below never sees a hub
    e.run("n1", &["agent", "add", "backend", "--role", "backend", "--runner", "claude-code"]);
    e.run("n1", &["agent", "add", "qa", "--role", "qa", "--runner", "codex"]);
    e.start_node("n1");
    assert_eq!(e.status("n1")["hub"], "offline");

    let qa = e.stream("n1", &["attach", "qa", "--json"]);
    expect(&qa, "qa attached", T, |v| v["event"] == "attached");
    let r = e.json("n1", &["send", "--as", "backend", "@qa@pc-one", "flaky test?", "--json"]);
    assert_eq!(r["local"], 1, "{r}");
    assert_eq!(r["remote"]["status"], "none");
    let m = expect(&qa, "local delivery to qa", T, |v| v["event"] == "message");
    assert_eq!(m["data"]["message"]["parts"][0]["text"], "flaky test?");
    let st = e.status("n1");
    assert_eq!(st["hub"], "offline");
    // Only the metadata journal waits for the hub; the message itself is done.
    assert_eq!(st["outbox"], 1, "{st}");
}

/// Criterion 5: a plain shell script using only the CLI with JSON can attach, be
/// announced, send and receive.
#[test]
fn shell_script_using_only_cli_json() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "ci-bot", "--role", "ci", "--runner", "script", "--mode", "script", "--project", "shop"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer", "--runner", "codex", "--project", "shop"]);
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");
    let rev = e.stream("n2", &["attach", "reviewer", "--json"]);
    expect(&rev, "reviewer attached", T, |v| v["event"] == "attached");

    let script = e.root.join("bot.sh");
    std::fs::write(
        &script,
        r#"#!/bin/sh
set -e
A="$AGENTLAN_BIN --home $N1"
$A attach ci-bot --json > "$OUT/attach.jsonl" &
ATTACH=$!
sleep 1
printf '%s' '{"to":{"kind":"handle","handle":"reviewer@pc-two"},"intent":"status","subject":"CI green","text":"all 42 tests passed"}' \
  | $A send --as ci-bot --stdin --json > "$OUT/send.json"
touch "$OUT/sent"
$A wait --as ci-bot --timeout 15 --json > "$OUT/wait.json"
kill $ATTACH
"#,
    )
    .unwrap();
    let out = e.root.join("out");
    std::fs::create_dir_all(&out).unwrap();
    let mut bot = Command::new("sh")
        .arg(&script)
        .env("AGENTLAN_BIN", BIN)
        .env("N1", e.home("n1"))
        .env("OUT", &out)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // The script's agent is announced to the other machine…
    expect(&rev, "ci-bot announced on n2", T, |v| v["event"] == "roster" && v["data"]["agent"]["handle"] == "ci-bot@pc-one");
    // …and its message arrives.
    let m = expect(&rev, "ci-bot message on n2", T, |v| v["event"] == "message");
    assert_eq!(m["data"]["message"]["subject"], "CI green");
    assert_eq!(m["data"]["message"]["intent"], "status");
    wait_until("script sent", T, || out.join("sent").exists());

    // Reply to the script; its `wait` receives it.
    let r = e.json("n2", &["send", "--as", "reviewer", "@ci-bot@pc-one", "thanks!", "--intent", "result", "--json"]);
    assert_eq!(r["remote"]["status"], "accepted", "{r}");
    let status = bot.wait().unwrap();
    let mut err = String::new();
    use std::io::Read;
    bot.stderr.take().map(|mut s| s.read_to_string(&mut err));
    assert!(status.success(), "script failed: {err}");

    let send: Value = serde_json::from_str(&std::fs::read_to_string(out.join("send.json")).unwrap()).unwrap();
    assert_eq!(send["remote"]["status"], "accepted", "{send}");
    let wait: Value = serde_json::from_str(&std::fs::read_to_string(out.join("wait.json")).unwrap()).unwrap();
    assert_eq!(wait["message"]["from"]["handle"], "reviewer@pc-two", "{wait}");
    assert_eq!(wait["message"]["parts"][0]["text"], "thanks!");
    let attach = std::fs::read_to_string(out.join("attach.jsonl")).unwrap();
    assert!(attach.lines().next().unwrap_or("").contains("\"attached\""), "{attach}");
}

/// Outbox: a cross-machine send made while the hub is down is delivered after it returns.
#[test]
fn queued_while_hub_down_then_delivered() {
    let mut e = Env::new();
    let hub = e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer"]);
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");
    e.kill(hub);
    wait_until("n1 sees hub down", T, || e.status("n1")["hub"] == "offline");
    let r = e.json("n1", &["send", "--as", "backend", "@reviewer@pc-two", "while you were out", "--id", "msg_e2e_queued", "--json"]);
    assert_eq!(r["remote"]["status"], "queued", "{r}");
    let wire_id = r["id"].as_str().unwrap().to_string();
    e.spawn("hub", &["hub", "run"]);
    wait_until("n1 outbox flushed", Duration::from_secs(15), || {
        let s = e.status("n1");
        s["hub"] == "ready" && s["outbox"] == 0
    });
    wait_until("delivered to reviewer", T, || {
        e.json("n2", &["inbox", "--as", "reviewer", "--json"])["messages"].as_array().unwrap().iter().any(|m| m["id"] == wire_id.as_str())
    });
}

/// A spawned `agentlan mcp` speaking JSON-RPC (newline-delimited) over stdio.
struct Mcp {
    stdin: std::process::ChildStdin,
    rx: mpsc::Receiver<Value>,
    next: u64,
}

impl Mcp {
    fn spawn(e: &mut Env, home: &str, agent: &str, runner: &str) -> Mcp {
        let mut child = e.cmd(home, &["mcp", "--agent", agent, "--runner", runner]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        let out = child.stdout.take().unwrap();
        e.kids.push(child);
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(out).lines().map_while(Result::ok) {
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    if tx.send(v).is_err() {
                        break;
                    }
                }
            }
        });
        Mcp { stdin, rx, next: 1 }
    }

    fn write(&mut self, v: Value) {
        use std::io::Write;
        writeln!(self.stdin, "{v}").unwrap();
        self.stdin.flush().unwrap();
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next;
        self.next += 1;
        self.write(serde_json::json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        let r = expect(&self.rx, &format!("response to {method}"), Duration::from_secs(20), |v| v["id"] == id);
        assert!(r.get("error").is_none(), "{method} failed: {r}");
        r["result"].clone()
    }

    fn init(&mut self) -> Value {
        let r = self.request("initialize", serde_json::json!({"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "e2e", "version": "0"}}));
        self.write(serde_json::json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        r
    }

    /// Fire a request without waiting for its response; returns the JSON-RPC id.
    fn start(&mut self, method: &str, params: Value) -> u64 {
        let id = self.next;
        self.next += 1;
        self.write(serde_json::json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        id
    }

    fn finish(&mut self, id: u64) -> Value {
        let r = expect(&self.rx, &format!("response {id}"), Duration::from_secs(30), |v| v["id"] == id);
        r["result"].clone()
    }

    fn tool(&mut self, name: &str, args: Value) -> Value {
        let r = self.request("tools/call", serde_json::json!({"name": name, "arguments": args}));
        assert_ne!(r["isError"], true, "{name} returned an error: {r}");
        r
    }
}

/// The MCP adapter end to end: Claude-style and Codex-style adapters on two PCs exchange a
/// request and a reply; each sees the same AMF/1 message rendered for its runner.
#[test]
fn mcp_adapters_claude_and_codex_roundtrip() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend", "--runner", "claude-code", "--project", "shop"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer", "--runner", "codex", "--project", "shop"]);
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");

    let mut rev = Mcp::spawn(&mut e, "n2", "reviewer", "codex");
    let init_rev = rev.init();
    assert!(init_rev["instructions"].as_str().unwrap().contains("reviewer@pc-two"), "{init_rev}");
    wait_until("reviewer online on n1", T, || {
        e.json("n1", &["ls", "--json"])["agents"].as_array().unwrap().iter().any(|a| a["handle"] == "reviewer@pc-two" && a["state"] == "online")
    });

    let mut back = Mcp::spawn(&mut e, "n1", "backend", "claude-code");
    let init = back.init();
    let instr = init["instructions"].as_str().unwrap();
    assert!(instr.contains("You are connected to AgentLAN as backend@pc-one"), "{instr}");
    assert!(instr.contains("reviewer@pc-two"), "roster in instructions: {instr}");
    let tools = back.request("tools/list", serde_json::json!({}));
    let names: Vec<&str> = tools["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    for t in ["whoami", "list_agents", "list_machines", "send", "reply", "inbox", "wait_for_message", "ack"] {
        assert!(names.contains(&t), "missing tool {t}: {names:?}");
    }

    // Claude → Codex request.
    let sent = back.tool("send", serde_json::json!({"to": "@reviewer@pc-two", "intent": "request", "subject": "PR 12", "text": "please review src/auth.rs"}));
    assert_eq!(sent["structuredContent"]["sent"]["remote"]["status"], "accepted", "{sent}");
    let req_id = sent["structuredContent"]["sent"]["id"].as_str().unwrap().to_string();

    // Codex receives it as JSON (content text mirrors structuredContent).
    let got = rev.tool("wait_for_message", serde_json::json!({"timeoutS": 15}));
    assert_eq!(got["structuredContent"]["messages"][0]["subject"], "PR 12", "{got}");
    assert_eq!(got["structuredContent"]["trust"], "untrusted-peer");
    let text: Value = serde_json::from_str(got["content"][0]["text"].as_str().unwrap()).expect("codex text is JSON");
    assert_eq!(text["messages"][0]["id"], req_id.as_str());

    // Codex replies; recipient and thread come from the original.
    let rep = rev.tool("reply", serde_json::json!({"messageId": req_id, "text": "lgtm, one nit on token expiry"}));
    assert_eq!(rep["structuredContent"]["sent"]["remote"]["status"], "accepted", "{rep}");

    // Claude reads it wrapped as untrusted peer content.
    let mut inbox = Value::Null;
    wait_until("reply in backend inbox", T, || {
        inbox = back.tool("inbox", serde_json::json!({}));
        inbox["structuredContent"]["messages"].as_array().is_some_and(|a| !a.is_empty())
    });
    let m = &inbox["structuredContent"]["messages"][0];
    assert_eq!(m["inReplyTo"], req_id.as_str());
    assert_eq!(m["subject"], "Re: PR 12");
    let txt = inbox["content"][0]["text"].as_str().unwrap();
    assert!(txt.contains("<agentlan_message from=\"reviewer@pc-two\"") && txt.contains("untrusted=\"true\""), "{txt}");
    let acked = back.tool("ack", serde_json::json!({"messageId": m["id"]}));
    assert_eq!(acked["structuredContent"]["unread"]["count"], 0, "{acked}");
}

fn inbox_subjects(e: &Env, home: &str, agent: &str) -> Vec<String> {
    e.json(home, &["inbox", "--as", agent, "--json"])["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["subject"].as_str().unwrap_or("").to_string())
        .collect()
}

fn send_draft(e: &Env, home: &str, as_agent: &str, draft: Value) -> Value {
    let mut child = e.cmd(home, &["send", "--as", as_agent, "--stdin", "--json"]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    {
        use std::io::Write;
        child.stdin.take().unwrap().write_all(draft.to_string().as_bytes()).unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "send failed: {}", String::from_utf8_lossy(&out.stderr));
    serde_json::from_slice(&out.stdout).unwrap()
}

/// Role, project, machine and pick-one fan-out across two PCs: local legs are delivered by
/// the node, remote legs by the hub, other projects and the sender are excluded.
#[test]
fn fanout_role_project_machine_and_pick_one() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    for (home, name, role, project) in [
        ("n1", "backend", "backend", "shop"),
        ("n1", "qa", "qa", "shop"),
        ("n1", "devops", "devops", "infra"),
        ("n2", "reviewer", "reviewer", "shop"),
        ("n2", "qa2", "qa", "shop"),
    ] {
        e.run(home, &["agent", "add", name, "--role", role, "--project", project]);
    }
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");
    // Broadcasts go to *attached* agents only.
    let _s: Vec<_> = [("n1", "backend"), ("n1", "qa"), ("n1", "devops"), ("n2", "reviewer"), ("n2", "qa2")]
        .iter()
        .map(|(h, a)| {
            let rx = e.stream(h, &["attach", a, "--json"]);
            expect(&rx, "attached", T, |v| v["event"] == "attached");
            rx
        })
        .collect();
    wait_until("remote agents online on n1", T, || {
        let agents = e.json("n1", &["ls", "--json"])["agents"].as_array().unwrap().clone();
        ["reviewer@pc-two", "qa2@pc-two"].iter().all(|h| agents.iter().any(|a| a["handle"] == *h && a["state"] == "online"))
    });

    // Project broadcast: local qa + remote reviewer, qa2; not devops (infra), not the sender.
    let r = send_draft(&e, "n1", "backend", serde_json::json!({"to": {"kind": "project", "project": "shop"}, "subject": "proj"}));
    assert_eq!(r["local"], 1, "{r}");
    assert_eq!(r["remote"], serde_json::json!({"status": "accepted", "count": 2}), "{r}");

    // Role broadcast: qa on both machines.
    let r = send_draft(&e, "n1", "backend", serde_json::json!({"to": {"kind": "role", "role": "qa"}, "subject": "role-qa"}));
    assert_eq!((r["local"].as_u64(), r["remote"]["count"].as_u64()), (Some(1), Some(1)), "{r}");

    // pick:one prefers a local match and skips the remote leg entirely.
    let r = send_draft(&e, "n1", "backend", serde_json::json!({"to": {"kind": "role", "role": "qa", "pick": "one"}, "subject": "pick-local"}));
    assert_eq!(r["local"], 1, "{r}");
    assert_eq!(r["remote"]["status"], "none", "{r}");

    // pick:one with no local match: exactly one remote recipient.
    let r = send_draft(&e, "n2", "qa2", serde_json::json!({"to": {"kind": "role", "role": "backend", "pick": "one"}, "subject": "pick-remote"}));
    assert_eq!((r["local"].as_u64(), r["remote"]["count"].as_u64()), (Some(0), Some(1)), "{r}");

    // Machine target: every attached agent on pc-two, nothing local.
    let r = send_draft(&e, "n1", "backend", serde_json::json!({"to": {"kind": "machine", "machine": "pc-two"}, "subject": "machine"}));
    assert_eq!((r["local"].as_u64(), r["remote"]["count"].as_u64()), (Some(0), Some(2)), "{r}");

    wait_until("remote deliveries settled", T, || inbox_subjects(&e, "n2", "reviewer").len() == 2 && inbox_subjects(&e, "n2", "qa2").len() == 3);
    let mut rv = inbox_subjects(&e, "n2", "reviewer");
    rv.sort();
    assert_eq!(rv, vec!["machine", "proj"]);
    let mut q2 = inbox_subjects(&e, "n2", "qa2");
    q2.sort();
    assert_eq!(q2, vec!["machine", "proj", "role-qa"]);
    let mut q = inbox_subjects(&e, "n1", "qa");
    q.sort();
    assert_eq!(q, vec!["pick-local", "proj", "role-qa"]);
    assert_eq!(inbox_subjects(&e, "n1", "backend"), vec!["pick-remote"], "sender excluded from its own broadcasts");
    assert!(inbox_subjects(&e, "n1", "devops").is_empty(), "other project excluded");
}


/// Request/reply over the CLI: a reply-waiter returns only the reply to its own request,
/// and unrelated mail that arrives meanwhile is left unseen for normal waits.
#[test]
fn request_reply_ignores_unrelated_mail() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer"]);
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");

    // A role target needs an online agent; nobody is attached yet, so it is refused.
    let none = e.cmd("n1", &["request", "--as", "backend", "role:reviewer", "anyone?", "--wait", "1"]).output().unwrap();
    assert!(!none.status.success() && String::from_utf8_lossy(&none.stderr).contains("NOT_DELIVERED"));

    // Direct mail reaches the reviewer even while it is offline.
    let req = e.cmd("n1", &["request", "--as", "backend", "@reviewer@pc-two", "is SKEW=30s right?", "--wait", "20", "--json"]).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let q = e.json("n2", &["wait", "--as", "reviewer", "--timeout", "10", "--json"]);
    let qid = q["message"]["id"].as_str().expect("reviewer received the request").to_string();
    assert_eq!(q["message"]["intent"], "request");
    assert!(q["message"]["replyBy"].is_string());

    // Unrelated mail first, then the actual reply.
    e.run("n2", &["send", "--as", "reviewer", "@backend@pc-one", "unrelated note", "--subject", "noise"]);
    std::thread::sleep(Duration::from_millis(300));
    e.run("n2", &["send", "--as", "reviewer", "@backend@pc-one", "yes, 30s covers clock drift", "--reply-to", &qid]);

    let out = req.wait_with_output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let r: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(r["status"], "replied", "{r}");
    assert_eq!(r["reply"]["inReplyTo"], qid.as_str());
    assert_eq!(r["reply"]["parts"][0]["text"], "yes, 30s covers clock drift");

    // The unrelated message was not consumed by the request.
    let w = e.json("n1", &["wait", "--as", "backend", "--timeout", "3", "--json"]);
    assert_eq!(w["message"]["subject"], "noise", "{w}");
}

/// MCP `request` round trip, and a cancelled `wait_for_message` does not lose the
/// message that arrives afterwards.
#[test]
fn mcp_request_tool_and_cancelled_wait() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend", "--runner", "claude-code"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer", "--runner", "codex"]);
    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");
    let mut back = Mcp::spawn(&mut e, "n1", "backend", "claude-code");
    back.init();
    let mut rev = Mcp::spawn(&mut e, "n2", "reviewer", "codex");
    rev.init();
    wait_until("reviewer online on n1", T, || {
        e.json("n1", &["ls", "--json"])["agents"].as_array().unwrap().iter().any(|a| a["handle"] == "reviewer@pc-two" && a["state"] == "online")
    });

    // backend asks (blocking tool call in flight)…
    let rid = back.start("tools/call", serde_json::json!({"name": "request", "arguments": {"to": "role:reviewer", "text": "approve PR 12?", "waitS": 20}}));
    // …reviewer receives it and replies.
    let got = rev.tool("wait_for_message", serde_json::json!({"timeoutS": 15}));
    let qid = got["structuredContent"]["messages"][0]["id"].as_str().unwrap().to_string();
    rev.tool("reply", serde_json::json!({"messageId": qid, "text": "approved"}));
    let res = back.finish(rid);
    assert_eq!(res["structuredContent"]["status"], "replied", "{res}");
    assert_eq!(res["structuredContent"]["reply"]["parts"][0]["text"], "approved");

    // Cancel a pending wait, then deliver: the next wait must still get the message.
    let wid = rev.start("tools/call", serde_json::json!({"name": "wait_for_message", "arguments": {"timeoutS": 30}}));
    std::thread::sleep(Duration::from_millis(300));
    rev.write(serde_json::json!({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": wid, "reason": "test"}}));
    std::thread::sleep(Duration::from_millis(500));
    back.tool("send", serde_json::json!({"to": "@reviewer@pc-two", "subject": "after-cancel", "text": "still there?"}));
    let again = rev.tool("wait_for_message", serde_json::json!({"timeoutS": 10}));
    assert_eq!(again["structuredContent"]["messages"][0]["subject"], "after-cancel", "{again}");
}

/// Run a Claude Code hook the way Claude does: JSON on stdin; stdout is JSON or empty.
fn hook(e: &Env, home: &str, event: &str, agent: &str, session: &str) -> Option<Value> {
    let mut child = e.cmd(home, &["hook", "claude", event, "--agent", agent]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    {
        use std::io::Write;
        let inp = serde_json::json!({"session_id": session, "hook_event_name": event, "cwd": "/tmp", "stop_hook_active": false});
        child.stdin.take().unwrap().write_all(inp.to_string().as_bytes()).unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "hook {event} failed: {}", String::from_utf8_lossy(&out.stderr));
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if s.is_empty() { None } else { Some(serde_json::from_str(&s).unwrap_or_else(|_| panic!("hook output is not JSON: {s}"))) }
}

/// Claude Code hooks: identity + peers at session start, pointer-only unread notices
/// (no peer-written text), no repeats, and Stop continuation only for worker profiles.
#[test]
fn claude_hooks_pointer_only_and_worker_only_stop() {
    let mut e = Env::new();
    e.network(&[("n1", "pc-one"), ("n2", "pc-two")]);
    e.run("n1", &["agent", "add", "backend", "--role", "backend", "--runner", "claude-code", "--project", "shop"]);
    e.run("n1", &["agent", "add", "bot", "--role", "fixer", "--runner", "claude-code", "--mode", "worker", "--project", "shop"]);
    e.run("n2", &["agent", "add", "reviewer", "--role", "reviewer", "--runner", "codex", "--project", "shop"]);

    // Node not running yet: hooks must stay silent and succeed.
    assert!(hook(&e, "n1", "session-start", "backend", "s0").is_none());

    e.start_node("n1");
    e.start_node("n2");
    wait_until("both nodes ready", T, || e.status("n1")["hub"] == "ready" && e.status("n2")["hub"] == "ready");
    let rev = e.stream("n2", &["attach", "reviewer", "--json"]);
    expect(&rev, "reviewer attached", T, |v| v["event"] == "attached");
    wait_until("reviewer online on n1", T, || {
        e.json("n1", &["ls", "--json"])["agents"].as_array().unwrap().iter().any(|a| a["handle"] == "reviewer@pc-two" && a["state"] == "online")
    });

    let ss = hook(&e, "n1", "session-start", "backend", "s1").expect("session-start context");
    assert_eq!(ss["hookSpecificOutput"]["hookEventName"], "SessionStart");
    let c = ss["hookSpecificOutput"]["additionalContext"].as_str().unwrap();
    assert!(c.contains("you are backend@pc-one") && c.contains("reviewer@pc-two"), "{c}");
    assert!(hook(&e, "n1", "user-prompt-submit", "backend", "s1").is_none(), "no unread → no output");

    // Mail arrives; the notice names sender/intent/id but never the subject.
    e.run("n2", &["send", "--as", "reviewer", "@backend@pc-one", "body text", "--subject", "SECRET-SUBJECT", "--intent", "request"]);
    e.run("n2", &["send", "--as", "reviewer", "@bot@pc-one", "fix it", "--subject", "job"]);
    wait_until("mail stored on n1", T, || e.status("n1")["outbox"] == 0 && hook(&e, "n1", "user-prompt-submit", "backend", "probe").is_some());
    let up = hook(&e, "n1", "user-prompt-submit", "backend", "s1").expect("pointer");
    let t = up["hookSpecificOutput"]["additionalContext"].as_str().unwrap();
    assert!(t.contains("1 unread") && t.contains("reviewer@pc-two") && t.contains("request"), "{t}");
    assert!(!t.contains("SECRET-SUBJECT") && !t.contains("body text"), "peer text leaked: {t}");

    // post-tool-use points out each new message once.
    assert!(hook(&e, "n1", "post-tool-use", "backend", "s2").is_some());
    assert!(hook(&e, "n1", "post-tool-use", "backend", "s2").is_none(), "no repeat for the same mail");

    // Stop: interactive profiles may always stop; workers continue up to the budget.
    assert!(hook(&e, "n1", "stop", "backend", "s3").is_none());
    for i in 0..3 {
        let st = hook(&e, "n1", "stop", "bot", "s4").unwrap_or_else(|| panic!("worker stop block #{i}"));
        assert_eq!(st["decision"], "block");
        assert!(!st["reason"].as_str().unwrap().contains("fix it"));
    }
    assert!(hook(&e, "n1", "stop", "bot", "s4").is_none(), "continuation budget exhausted");

    // Peeking never consumed the mail.
    let w = e.json("n1", &["wait", "--as", "backend", "--timeout", "3", "--json"]);
    assert_eq!(w["message"]["subject"], "SECRET-SUBJECT");

    // Settings snippet: Stop only for worker profiles.
    let p_bot = String::from_utf8(e.run("n1", &["hooks", "print", "claude", "--agent", "bot"]).stdout).unwrap();
    let p_back = String::from_utf8(e.run("n1", &["hooks", "print", "claude", "--agent", "backend"]).stdout).unwrap();
    assert!(p_bot.contains("\"Stop\"") && !p_back.contains("\"Stop\""));
    assert!(serde_json::from_str::<Value>(&p_back).unwrap()["hooks"]["PostToolUse"][0]["matcher"] == "*");
}


/// Loop guard: a thread pauses after its automated-message budget, the pause survives a
/// node restart, resuming needs a human at a terminal, and per-pair rate limits apply.
#[test]
fn loop_guard_pauses_threads_and_limits_pairs() {
    let mut e = Env::new();
    e.extra_env = vec![("AGENTLAN_LIMIT_THREAD", "3".into()), ("AGENTLAN_LIMIT_PAIR_HOUR", "6".into())];
    e.network(&[("n1", "pc-one")]);
    e.run("n1", &["agent", "add", "a", "--role", "dev"]);
    e.run("n1", &["agent", "add", "b", "--role", "dev"]);
    let node = e.start_node("n1");

    let send = |e: &Env, thread: &str| e.cmd("n1", &["send", "--as", "a", "@b@pc-one", "ping", "--thread", thread]).output().unwrap();
    for _ in 0..3 {
        assert!(send(&e, "thr_loop").status.success());
    }
    let paused = send(&e, "thr_loop");
    assert!(!paused.status.success());
    assert!(String::from_utf8_lossy(&paused.stderr).contains("PAUSED"), "{}", String::from_utf8_lossy(&paused.stderr));
    assert_eq!(e.status("n1")["pausedThreads"][0]["threadId"], "thr_loop");

    // The pause is persisted: still paused after a node restart.
    e.kill(node);
    e.start_node("n1");
    assert!(String::from_utf8_lossy(&send(&e, "thr_loop").stderr).contains("PAUSED"));

    // Resuming refuses without a terminal (an agent can run the CLI too)…
    let no_tty = e.cmd("n1", &["thread", "resume", "thr_loop"]).stdin(Stdio::null()).output().unwrap();
    assert!(!no_tty.status.success() && String::from_utf8_lossy(&no_tty.stderr).contains("interactive terminal"));
    // …and works for a human at a (pseudo-)terminal who confirms.
    let mut pty = Command::new("python3")
        .args(["-c", "import pty,sys; sys.exit(pty.spawn(sys.argv[1:]) >> 8)", BIN, "--home"])
        .arg(e.home("n1"))
        .args(["thread", "resume", "thr_loop"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    {
        use std::io::Write;
        std::thread::sleep(Duration::from_millis(500));
        pty.stdin.take().unwrap().write_all(b"y\n").unwrap();
    }
    let out = pty.wait_with_output().unwrap();
    assert!(String::from_utf8_lossy(&out.stdout).contains("resumed thr_loop"), "{}", String::from_utf8_lossy(&out.stdout));
    assert!(send(&e, "thr_loop").status.success(), "thread usable again after resume");

    // Per-pair hourly limit: 4 sends so far to b; two more fit, the next is rate-limited.
    assert!(send(&e, "thr_x").status.success());
    assert!(send(&e, "thr_y").status.success());
    let limited = send(&e, "thr_z");
    assert!(String::from_utf8_lossy(&limited.stderr).contains("RATE_LIMITED"), "{}", String::from_utf8_lossy(&limited.stderr));
}
