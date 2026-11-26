#!/bin/sh
# AgentLAN installer for macOS and Linux.
#
#   curl -fsSL https://raw.githubusercontent.com/jenn0pal/agentlan/main/install.sh | sh
#
# Downloads the prebuilt binary for this machine from GitHub Releases, verifies its
# SHA-256 checksum and installs it. Falls back to building from source with cargo
# when no prebuilt binary matches.
#
# Options (environment variables):
#   AGENTLAN_VERSION=v0.1.0      install a specific release (default: latest)
#   AGENTLAN_INSTALL_DIR=~/bin   where to put the binary (default: ~/.local/bin)
#   AGENTLAN_FROM_SOURCE=1       build from source with cargo instead of downloading
set -eu

REPO="jenn0pal/agentlan"
VERSION="${AGENTLAN_VERSION:-latest}"
INSTALL_DIR="${AGENTLAN_INSTALL_DIR:-$HOME/.local/bin}"
BIN=""

say() { printf '%s\n' "$*"; }
err() { printf 'agentlan-install: error: %s\n' "$*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

download() { # <url> <dest>
  if have curl; then
    curl -fsSL --retry 3 -o "$2" "$1"
  elif have wget; then
    wget -q -O "$2" "$1"
  else
    err "curl or wget is required"
  fi
}

sha256_of() {
  if have sha256sum; then sha256sum "$1" | awk '{print $1}'
  elif have shasum; then shasum -a 256 "$1" | awk '{print $1}'
  else echo ""
  fi
}

detect_target() {
  arch=$(uname -m)
  case "$arch" in
    x86_64 | amd64) arch=x86_64 ;;
    arm64 | aarch64) arch=aarch64 ;;
    *) return 1 ;;
  esac
  case "$(uname -s)" in
    Darwin) echo "$arch-apple-darwin" ;;
    Linux) echo "$arch-unknown-linux-musl" ;; # static binary: any distro
    *) return 1 ;;
  esac
}

install_bin() { # <built binary> — same destination as a download
  mkdir -p "$INSTALL_DIR" || err "cannot create $INSTALL_DIR"
  cp "$1" "$INSTALL_DIR/agentlan.new" || err "cannot write to $INSTALL_DIR"
  chmod 755 "$INSTALL_DIR/agentlan.new" || err "cannot make the binary executable"
  mv -f "$INSTALL_DIR/agentlan.new" "$INSTALL_DIR/agentlan" || err "cannot install into $INSTALL_DIR"
  BIN="$INSTALL_DIR/agentlan"
}

from_source() {
  have cargo || err "no prebuilt binary for this system, and Rust is not installed. Install Rust from https://rustup.rs, then run: AGENTLAN_FROM_SOURCE=1 sh install.sh"
  say "Building agentlan from source with cargo (this takes a few minutes)…"
  root=$(mktemp -d)
  if [ "$VERSION" = latest ]; then
    cargo install --locked --root "$root" --git "https://github.com/$REPO" agentlan || err "cargo install failed"
  else
    cargo install --locked --root "$root" --git "https://github.com/$REPO" --tag "$VERSION" agentlan || err "cargo install failed"
  fi
  # Install the binary we just built (not whatever `agentlan` happens to be on PATH).
  install_bin "$root/bin/agentlan"
  rm -rf "$root"
}

# GitHub's /releases/latest skips pre-releases; the releases API lists them too (newest first).
resolve_version() {
  if [ "$VERSION" != latest ]; then
    echo "$VERSION"
    return 0
  fi
  j=$(mktemp)
  download "https://api.github.com/repos/$REPO/releases?per_page=1" "$j" || { rm -f "$j"; return 1; }
  tag=$(sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' "$j" | head -n 1)
  rm -f "$j"
  [ -n "$tag" ] && echo "$tag"
}

# Returns 1 only when no prebuilt binary is available (caller falls back to a source
# build). Any other failure is fatal: under `from_release || from_source`, `set -e` is
# off inside this function, so every step checks for errors explicitly.
from_release() { # <target>
  target="$1"
  if tag=$(resolve_version); then
    base="https://github.com/$REPO/releases/download/$tag"
  else
    say "Could not query the releases API; trying the latest stable release."
    tag="latest"
    base="https://github.com/$REPO/releases/latest/download"
  fi
  asset="agentlan-$target.tar.gz"
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  say "Downloading $asset ($tag)…"
  if ! download "$base/$asset" "$tmp/$asset"; then
    say "No prebuilt binary found for $target."
    return 1
  fi
  download "$base/$asset.sha256" "$tmp/$asset.sha256" || err "could not download the checksum for $asset"
  want=$(awk '{print $1}' "$tmp/$asset.sha256")
  got=$(sha256_of "$tmp/$asset")
  [ -n "$got" ] || err "need sha256sum or shasum to verify the download"
  [ "$want" = "$got" ] || err "checksum mismatch for $asset (expected $want, got $got)"
  say "Checksum verified."
  tar -xzf "$tmp/$asset" -C "$tmp" || err "could not extract $asset"
  [ -f "$tmp/agentlan" ] || err "$asset does not contain the agentlan binary"
  install_bin "$tmp/agentlan"
}

main() {
  if [ "${AGENTLAN_FROM_SOURCE:-0}" = 1 ]; then
    from_source
  elif target=$(detect_target); then
    from_release "$target" || from_source
  else
    say "No prebuilt binary for $(uname -s)/$(uname -m)."
    from_source
  fi

  say ""
  say "✓ Installed $("$BIN" --version) → $BIN"
  dir=$(dirname "$BIN")
  case ":$PATH:" in
    *":$dir:"*) ;;
    *)
      case "${SHELL:-}" in
        */zsh) rc="$HOME/.zshrc" ;;
        */bash) rc="$HOME/.bashrc" ;;
        *) rc="$HOME/.profile" ;;
      esac
      say ""
      say "$dir is not on your PATH yet. Add it with:"
      say "  echo 'export PATH=\"$dir:\$PATH\"' >> $rc && . $rc"
      ;;
  esac
  cat <<'EOF'

Next steps:
  On the PC you pick as the server:   agentlan up
  On every other PC:                  agentlan connect <code printed by `agentlan up`>
  Add an agent on a PC:               agentlan agent add backend --role backend --runner claude-code
  Connect Claude Code to it:          claude mcp add -s local agentlan -- agentlan mcp --agent backend --runner claude-code

Docs: https://github.com/jenn0pal/agentlan
EOF
}

main "$@"
