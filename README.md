# AgentLAN

[![CI](https://github.com/jenn0pal/agentlan/actions/workflows/ci.yml/badge.svg)](https://github.com/jenn0pal/agentlan/actions/workflows/ci.yml)

**A LAN intercom for AI coding agents.** AgentLAN links Claude Code, Codex and your own scripts running on different Mac and Windows PCs, so they can find each other and talk. Every agent has a recorded identity, a role and a project, and the network announces it when the agent connects. Agents on the same PC talk directly through a local daemon, without touching the network.

> **Status: alpha (v0.1).**
> - **Built and tested on macOS:** the core system and the MCP adapter.
> - **Not yet built on Windows**, though the code is written to be cross-platform.
> - **Not yet tested across two physical machines.** End-to-end tests run every process on one machine over loopback.
>
> Expect breaking changes before 1.0.

## Features

**Connecting**
- **One Rust binary.** Any PC can be the server. Every PC runs a small daemon, and each AI agent gets an MCP adapter.
- **Identity and roster.** Each agent has a permanent `agt_…` id, a role, a project, a runner and its machine. A newcomer receives the roster, its own project first, with same-machine peers flagged.
- **Machine details.** The registry records each PC's OS, CPU, RAM and addresses (shared by default; set `"shareHardware": false` in `node/node.json` to turn it off), so agents can choose peers sensibly.

**Messaging**
- **Same-PC routing.** Messages between agents on one PC go through the local daemon. They never cross the network and keep working when the server is down.
- **Durable cross-PC messaging.**
  - Messages are stored and forwarded, and offline mail waits up to 24 hours.
  - Delivery is in order per sender.
  - Retry keys stop retries from duplicating messages.
  - A queued outbox drains when the server comes back.
- **AMF/1, one runner-agnostic message format.** Its parts use the same shape as the [A2A](https://a2a-protocol.org) protocol. Small translators at the edge render it per runner:
  - Claude Code sees peer messages wrapped and marked untrusted;
  - Codex gets JSON;
  - scripts use the CLI with JSON in and out.

**Security and networking**
- **Encrypted, authenticated links.** Every link uses the [Noise](https://noiseprotocol.org) XX handshake with a pinned server key and one-time join codes.
- **Tailscale-aware.** `hub invite --tailscale` advertises this PC's MagicDNS name, so agents can link across sites over your tailnet.

## How it works

```
  PC-A (macOS)                           PC-B (any PC: server)               PC-C (Windows)
┌───────────────────────────┐          ┌────────────────────────┐          ┌───────────────────────────┐
│ Claude Code     Codex     │          │ agentlan hub           │          │ Codex          script     │
│  └ agentlan mcp └ agentlan mcp       │  registry · presence   │          │  └ agentlan mcp └ CLI JSON│
│        │ local IPC │      │  Noise   │  routing · mailboxes   │  Noise   │        │ local IPC │      │
│   ┌──────────────────┐    │  over    └───────────▲────────────┘  over    │   ┌──────────────────┐    │
│   │  agentlan node   │────┼──── TCP ─────────────┴───────────── TCP ─────┼───│  agentlan node   │    │
│   │  (local router)  │    │                                              │   │  (local router)  │    │
│   └──────────────────┘    │                                              │   └──────────────────┘    │
└───────────────────────────┘                                              └───────────────────────────┘
   same-PC messages stay inside the daemon; cross-PC messages go node → hub → node
```

For the full design, see [`PROPOSAL.md`](PROPOSAL.md). For an animated walkthrough, open [`process-flow.html`](process-flow.html) in a browser.

## Quick start

You need Rust 1.89 or newer.

```sh
cargo build --release               # → target/release/agentlan
cargo test --workspace              # unit tests + end-to-end tests (real processes on 127.0.0.1)
```

**1. On the PC you choose as the server**

```sh
agentlan hub init --name home-lab
agentlan hub run                       # keep running (TCP 47820)
agentlan hub invite                    # one-time join code per PC (valid 15 min)
agentlan hub invite --tailscale        # …or advertise only this PC's Tailscale name
```

**2. On every PC, including the server PC if agents run there**

```sh
agentlan join AGENTLAN1-…
agentlan agent add backend --role backend --runner claude-code --project github.com/acme/shop-api
agentlan node run                      # keep running
```

**3. Look around and send a message**

```sh
agentlan ls                            # agents: role, project, presence, same-PC flag
agentlan machines                      # PCs: OS, CPU, RAM, agents online
agentlan send --as backend @reviewer@win-pc "PR 12 is ready" --subject "review please"
agentlan inbox --as reviewer
```

## Connect your AI agents (MCP)

```sh
# Claude Code (per project)
claude mcp add -s local agentlan -- agentlan mcp --agent backend --runner claude-code
# Codex CLI (picks the profile from runner + git remote)
codex mcp add agentlan -- agentlan mcp --agent auto --runner codex
```

**Tools:** `whoami`, `list_agents`, `list_machines`, `send`, `reply`, `inbox`, `wait_for_message`, `ack`.

**Annotations.** Tools carry MCP hints:
- read-only: `whoami`, `list_agents`, `list_machines`;
- local only: `inbox`, `wait_for_message`, `ack`;
- reaches other agents: `send`, `reply`.

**Codex** (verified with Codex CLI 1.x):
- **Local tools** run without prompting.
- **`send` / `reply` ask for approval**, so your agent won't message others behind your back.
- **Unattended worker sessions** can opt in per server in `~/.codex/config.toml`:

  ```toml
  [mcp_servers.agentlan]
  command = "agentlan"
  args = ["mcp", "--agent", "auto", "--runner", "codex"]
  tool_timeout_sec = 90                       # wait_for_message blocks up to 45 s
  # default_tools_approval_mode = "approve"   # worker sessions only: no approval prompts
  ```

- **AgentLAN's server instructions** (identity and peer list) reach Codex when it discovers the tools.
- **`--agent auto`** works because Codex starts the adapter in the session's working directory.

**Scripts and other agents** use the CLI:

```sh
agentlan attach ci-bot --json &        # announce + stream events as JSON lines
echo '{"to":{"kind":"role","role":"qa"},"intent":"status","subject":"CI green"}' \
  | agentlan send --as ci-bot --stdin --json
agentlan wait --as ci-bot --json
```

## Over Tailscale

AgentLAN works across a tailnet: the same PCs, or PCs at different sites, with no ports opened to the internet.

- **Choosing addresses.** `hub invite --tailscale` advertises only the MagicDNS name and the 100.x address. The default `hub invite` lists LAN addresses first, then Tailscale, and skips VM/Docker bridges, WSL/Hyper-V switches and other VPN tunnels. The chosen addresses are printed to stderr.
- **Tailscale access rules** must allow TCP 47820 to the server PC. The default allow-all policy does.

## Security model

- **Network membership.** PCs join with a one-time code that contains the server's full public key, so a fake server is rejected during the handshake. The server then pins each PC's key.
- **Peer messages are untrusted data.** They are never instructions. The protocol has no "execute" verb. Adapters mark peer content as untrusted, and a peer cannot close Claude's wrapper early.
- **Auto-wake is off by default.** Agents can't start work in another agent's session unless that profile is a dedicated worker. See §7.5 of the proposal for the impact analysis.
- **Same OS user.** On macOS and Linux, the local IPC socket lives in a private (0700) directory, so only your user can reach it. On Windows, restricting the named pipe to the current user is still to do. Any process running as your user can act as your agents.

## Configuration

| | |
|---|---|
| **Data directory** | The per-user app-data folder by default (`--home DIR` or `AGENTLAN_HOME` overrides it). It holds `hub/` (`hub.key`, `hub.json`, `hub.db`) and `node/` (`node.key`, `node.json`, `profiles.json`, `node.db`, plus the IPC socket). |
| **Environment variables** | `AGENTLAN_LOG` (log level), `AGENTLAN_HEARTBEAT_MS`, `AGENTLAN_LEASE_MS`, `AGENTLAN_BACKOFF_MAX_MS` |

## Project layout

| Crate | Purpose |
|---|---|
| `agentlan-proto` | AMF/1 message format, registry records, wire frames, IPC protocol, join codes (no I/O) |
| `agentlan-net` | Noise XX handshake and chunked, encrypted framing |
| `agentlan-store` | SQLite helpers (WAL, `synchronous=FULL`) |
| `agentlan-hub` | Server: registry, sequenced events, fenced sessions, transactional routing, in-order delivery pump |
| `agentlan-node` | Per-PC daemon: local router, durable inbox/outbox, server link, interface classification |
| `agentlan-mcp` | MCP stdio adapter ([rmcp](https://crates.io/crates/rmcp)) with per-runner translators |
| `agentlan` | The single binary: CLI, `hub`, `node`, `mcp` |

## Roadmap

**Phase 2 (in progress)**
- Request/reply that waits for the answer.
- Claude Code hooks.
- Opt-in wake for worker agents.
- Loop-guard budgets.
- Tests for role and project broadcasts.

**Phase 3**
- Background services: macOS LaunchAgent and Windows startup task.
- mDNS discovery.
- Moving the server to another PC, and server backups.
- `doctor` command and a terminal dashboard.
- Installers and code signing.

**Still to verify:** Windows builds, and a real test across two machines (LAN and Tailscale).

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
