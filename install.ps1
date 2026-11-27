# AgentLAN installer for Windows (PowerShell).
#
#   irm https://raw.githubusercontent.com/jenn0pal/agentlan/main/install.ps1 | iex
#
# Downloads the prebuilt binary from GitHub Releases, verifies its SHA-256 checksum,
# installs it and adds it to your user PATH.
#
# Options (environment variables):
#   $env:AGENTLAN_VERSION = 'v0.1.0'           install a specific release (default: latest)
#   $env:AGENTLAN_INSTALL_DIR = 'C:\tools\agl'  where to put the binary
#                                               (default: %LOCALAPPDATA%\Programs\agentlan)
#   $env:AGENTLAN_FROM_SOURCE = '1'            build from source with cargo instead of downloading

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$Repo = 'jenn0pal/agentlan'
$Version = if ($env:AGENTLAN_VERSION) { $env:AGENTLAN_VERSION } else { 'latest' }
$InstallDir = if ($env:AGENTLAN_INSTALL_DIR) { $env:AGENTLAN_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA 'Programs\agentlan' }
$Target = 'x86_64-pc-windows-msvc'   # also runs on Windows on Arm (x64 emulation)
$Asset = "agentlan-$Target.zip"
# Was AgentLAN running when the install started (background node, an agent's MCP server, `agentlan up`)?
$WasRunning = @(Get-Process -Name agentlan -ErrorAction SilentlyContinue).Count -gt 0

function Install-Bin([string]$Built) {
    New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
    $Dest = Join-Path $InstallDir 'agentlan.exe'
    # Windows can't overwrite a running .exe, but it can rename one: move the old binary
    # aside (running copies keep working) and put the new one in its place. Asides left by
    # earlier upgrades are removed once nothing runs them any more.
    Get-ChildItem -Path $InstallDir -Filter 'agentlan.old-*.exe' -ErrorAction SilentlyContinue |
        Remove-Item -Force -ErrorAction SilentlyContinue
    $Old = $null
    if (Test-Path -LiteralPath $Dest) {
        $Old = Join-Path $InstallDir ('agentlan.old-' + [Guid]::NewGuid().ToString('N').Substring(0, 8) + '.exe')
        Move-Item -LiteralPath $Dest -Destination $Old
    }
    try {
        Copy-Item -LiteralPath $Built -Destination $Dest -Force
    }
    catch {
        if ($Old -and -not (Test-Path -LiteralPath $Dest)) { Move-Item -LiteralPath $Old -Destination $Dest }
        throw
    }
    if ($Old) { Remove-Item -LiteralPath $Old -Force -ErrorAction SilentlyContinue }
}

function Install-FromSource {
    if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
        throw 'Rust is not installed. Install it from https://rustup.rs, then set AGENTLAN_FROM_SOURCE=1 and run this again.'
    }
    Write-Host 'Building agentlan from source with cargo (this takes a few minutes)...'
    $Root = Join-Path ([IO.Path]::GetTempPath()) ("agentlan-src-" + [Guid]::NewGuid().ToString('N'))
    $CargoArgs = @('install', '--locked', '--root', $Root, '--git', "https://github.com/$Repo")
    if ($Version -ne 'latest') { $CargoArgs += @('--tag', $Version) }
    $CargoArgs += 'agentlan'
    & cargo @CargoArgs
    if ($LASTEXITCODE -ne 0) { throw 'cargo install failed' }
    Install-Bin (Join-Path $Root 'bin\agentlan.exe')
    Remove-Item -Recurse -Force $Root -ErrorAction SilentlyContinue
}

function Install-FromRelease {
    $Tag = $Version
    if ($Tag -eq 'latest') {
        # /releases/latest skips pre-releases; the releases API lists them too (newest first).
        $Releases = Invoke-RestMethod -UseBasicParsing -Uri "https://api.github.com/repos/$Repo/releases?per_page=1" -Headers @{ 'User-Agent' = 'agentlan-installer' }
        $Tag = @($Releases)[0].tag_name
        if (-not $Tag) { throw 'no AgentLAN release found' }
    }
    $Base = "https://github.com/$Repo/releases/download/$Tag"
    $Tmp = Join-Path ([IO.Path]::GetTempPath()) ("agentlan-" + [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $Tmp | Out-Null
    try {
        Write-Host "Downloading $Asset ($Tag)..."
        Invoke-WebRequest -UseBasicParsing -Uri "$Base/$Asset" -OutFile (Join-Path $Tmp $Asset)
        Invoke-WebRequest -UseBasicParsing -Uri "$Base/$Asset.sha256" -OutFile (Join-Path $Tmp "$Asset.sha256")
        $want = ((Get-Content (Join-Path $Tmp "$Asset.sha256") -Raw).Trim() -split '\s+')[0].ToLower()
        $got = (Get-FileHash (Join-Path $Tmp $Asset) -Algorithm SHA256).Hash.ToLower()
        if ($want -ne $got) { throw "checksum mismatch for $Asset (expected $want, got $got)" }
        Write-Host 'Checksum verified.'
        Expand-Archive -Path (Join-Path $Tmp $Asset) -DestinationPath $Tmp -Force
        Install-Bin (Join-Path $Tmp 'agentlan.exe')
    }
    finally {
        Remove-Item -Recurse -Force $Tmp -ErrorAction SilentlyContinue
    }
}

if ($env:AGENTLAN_FROM_SOURCE -eq '1') { Install-FromSource } else { Install-FromRelease }

$UserPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if (-not (($UserPath -split ';') -contains $InstallDir)) {
    $NewPath = if ($UserPath) { "$UserPath;$InstallDir" } else { $InstallDir }
    [Environment]::SetEnvironmentVariable('Path', $NewPath, 'User')
    $env:Path = "$env:Path;$InstallDir"
    Write-Host "Added $InstallDir to your user PATH (new terminals pick it up automatically)."
}

$Exe = Join-Path $InstallDir 'agentlan.exe'
Write-Host ''
Write-Host ("Installed " + (& $Exe --version) + " -> $Exe")
if ($WasRunning) {
    Write-Host ''
    Write-Host 'AgentLAN was running. Running copies keep the old version until they restart:'
    Write-Host '  run "agentlan node stop", restart your agent sessions, and restart "agentlan up" if this PC is the server.'
}
Write-Host @'

Next steps:
  On the PC you pick as the server:   agentlan up   (prints a join code)
  Add the MCP tool for your agents:   claude mcp add -s user agentlan -- agentlan mcp
                                      codex mcp add agentlan -- agentlan mcp
  Then tell an agent:  "Connect to AgentLAN with code AGENTLAN1-... and register as <role>."
  (Only the first session on each PC needs the code; each code works for one PC.)

If this PC is the server, allow agentlan through Windows Firewall when prompted (TCP 47820, Private networks).
Docs: https://github.com/jenn0pal/agentlan
'@
