# ailink

Link AI coding agents (Claude Code, Codex, scripts…) across Mac and Windows PCs on your LAN.
Design: [`PROPOSAL.md`](PROPOSAL.md) · interactive flow: [`process-flow.html`](process-flow.html).

**Status: Milestone 1.**

| Works and is tested on macOS | Not built yet |
|---|---|
| • server (hub) on any PC<br>• join codes<br>• Noise-encrypted links<br>• machine and agent registry<br>• roster on connect and join/leave announcements<br>• same-PC routing through the local daemon (works with the server down)<br>• cross-PC messages with store-and-forward, outbox and retry de-duplication<br>• CLI with JSON in and out | • MCP adapter<br>• mDNS discovery<br>• Claude hooks<br>• auto-wake<br>• request/reply<br>• `hub move`<br>• installers |

**Windows:** the code is written to be cross-platform, but it has not been built or run on Windows yet.

## Build

```sh
cargo build --release        # → target/release/ailink
cargo test --workspace       # unit tests + end-to-end tests (real processes on 127.0.0.1)
```

## Try it on one LAN

Every PC uses the same `ailink` binary.

```sh
# 1. On the PC you pick as the server
ailink hub init --name home-lab
ailink hub run                      # keep running (TCP 47820; on Windows allow it through the firewall)
ailink hub invite                   # prints AILINK1-…  (one code per PC, valid 15 min)

# 2. On every PC, including the server PC if agents run there
ailink join AILINK1-…               # pins the server key, enrols this PC
ailink agent add backend --role backend --runner claude-code --project github.com/acme/shop-api
ailink node run                     # keep running

# 3. Use it
ailink ls                           # who is online, roles, projects, same-PC flag
ailink machines                     # PCs with OS / CPU / RAM
ailink attach backend               # hold a session and stream events (Ctrl-C detaches)
ailink send --as backend @reviewer@win-pc "PR 12 is ready" --subject "review please"
ailink inbox --as reviewer
ailink ack --as reviewer msg_…
ailink wait --as reviewer --timeout 30
```

**Scripts and other agents.** Add `--json` to get JSON out, and use `send --stdin` to pass an AMF/1 draft in:

```sh
echo '{"to":{"kind":"role","role":"qa"},"intent":"status","subject":"CI green"}' \
  | ailink send --as ci-bot --stdin --json
```

## Data location

The default location is the per-user app-data directory. Override it with `--home DIR` or `AILINK_HOME`.

| Path | Contents |
|---|---|
| `hub/` | `hub.key`, `hub.json`, `hub.db` |
| `node/` | `node.key`, `node.json`, `profiles.json`, `node.db`, and `node.sock` on macOS/Linux (a named pipe on Windows) |

## Development knobs

| Variable | Purpose |
|---|---|
| `AILINK_LOG=info` | Log verbosity |
| `AILINK_HEARTBEAT_MS` | Heartbeat interval |
| `AILINK_LEASE_MS` | How long before a silent PC counts as lost |
| `AILINK_BACKOFF_MAX_MS` | Maximum reconnect backoff |
