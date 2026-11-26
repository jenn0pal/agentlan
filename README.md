# agentlan

Link AI coding agents (Claude Code, Codex, scripts…) across Mac and Windows PCs on your LAN.
Design: [`PROPOSAL.md`](PROPOSAL.md) · interactive flow: [`process-flow.html`](process-flow.html).

**Status: Milestone 1, plus the MCP adapter.**

| Works and is tested on macOS | Not built yet |
|---|---|
| • server (hub) on any PC<br>• join codes<br>• Noise-encrypted links<br>• machine and agent registry<br>• roster on connect and join/leave announcements<br>• same-PC routing through the local daemon (works with the server down)<br>• cross-PC messages with store-and-forward, outbox and retry de-duplication<br>• CLI with JSON in and out<br>• MCP adapter for Claude Code (XML-wrapped text) and Codex (JSON) | • mDNS discovery<br>• Claude hooks<br>• auto-wake<br>• request/reply<br>• `hub move`<br>• installers |

**Windows:** the code is written to be cross-platform, but it has not been built or run on Windows yet.

## Build

```sh
cargo build --release        # → target/release/agentlan   (Rust ≥ 1.89)
cargo test --workspace       # unit tests + end-to-end tests (real processes on 127.0.0.1)
```

## Try it on one LAN

Every PC uses the same `agentlan` binary.

```sh
# 1. On the PC you pick as the server
agentlan hub init --name home-lab
agentlan hub run                      # keep running (TCP 47820; on Windows allow it through the firewall)
agentlan hub invite                   # prints AGENTLAN1-…  (one code per PC, valid 15 min)

# 2. On every PC, including the server PC if agents run there
agentlan join AGENTLAN1-…               # pins the server key, enrols this PC
agentlan agent add backend --role backend --runner claude-code --project github.com/acme/shop-api
agentlan node run                     # keep running

# 3. Use it
agentlan ls                           # who is online, roles, projects, same-PC flag
agentlan machines                     # PCs with OS / CPU / RAM
agentlan attach backend               # hold a session and stream events (Ctrl-C detaches)
agentlan send --as backend @reviewer@win-pc "PR 12 is ready" --subject "review please"
agentlan inbox --as reviewer
agentlan ack --as reviewer msg_…
agentlan wait --as reviewer --timeout 30
```

## Connect an AI agent (MCP)

```sh
# Claude Code (per project): Claude sees peer messages wrapped as <agentlan_message … untrusted="true">
claude mcp add -s local agentlan -- agentlan mcp --agent backend --runner claude-code
# Codex CLI: Codex gets JSON (structuredContent mirrored in the text)
codex mcp add agentlan -- agentlan mcp --agent auto --runner codex
```

**Tools:** `whoami`, `list_agents`, `list_machines`, `send`, `reply`, `inbox`, `wait_for_message` (≤ 45 s), `ack`.

**Result format:** every result carries structured AMF/1 JSON and an unread summary.

**`--agent auto`** picks the profile whose runner matches and whose `--project` equals the git remote of the working directory.

**Scripts and other agents.** Add `--json` to get JSON out, and use `send --stdin` to pass an AMF/1 draft in:

```sh
echo '{"to":{"kind":"role","role":"qa"},"intent":"status","subject":"CI green"}' \
  | agentlan send --as ci-bot --stdin --json
```

## Data location

The default location is the per-user app-data directory. Override it with `--home DIR` or `AGENTLAN_HOME`.

| Path | Contents |
|---|---|
| `hub/` | `hub.key`, `hub.json`, `hub.db` |
| `node/` | `node.key`, `node.json`, `profiles.json`, `node.db`, and `node.sock` on macOS/Linux (a named pipe on Windows) |

## Development knobs

| Variable | Purpose |
|---|---|
| `AGENTLAN_LOG=info` | Log verbosity |
| `AGENTLAN_HEARTBEAT_MS` | Heartbeat interval |
| `AGENTLAN_LEASE_MS` | How long before a silent PC counts as lost |
| `AGENTLAN_BACKOFF_MAX_MS` | Maximum reconnect backoff |
