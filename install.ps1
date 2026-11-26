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

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$Repo = 'jenn0pal/agentlan'
$Version = if ($env:AGENTLAN_VERSION) { $env:AGENTLAN_VERSION } else { 'latest' }
$InstallDir = if ($env:AGENTLAN_INSTALL_DIR) { $env:AGENTLAN_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA 'Programs\agentlan' }
$Target = 'x86_64-pc-windows-msvc'   # also runs on Windows on Arm (x64 emulation)
$Asset = "agentlan-$Target.zip"
if ($Version -eq 'latest') {
    # /releases/latest skips pre-releases; the releases API lists them too (newest first).
    $Releases = Invoke-RestMethod -UseBasicParsing -Uri "https://api.github.com/repos/$Repo/releases?per_page=1" -Headers @{ 'User-Agent' = 'agentlan-installer' }
    $Version = @($Releases)[0].tag_name
    if (-not $Version) { throw 'no AgentLAN release found' }
}
$Base = "https://github.com/$Repo/releases/download/$Version"

$Tmp = Join-Path ([IO.Path]::GetTempPath()) ("agentlan-" + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $Tmp | Out-Null
try {
    Write-Host "Downloading $Asset ($Version)..."
    Invoke-WebRequest -UseBasicParsing -Uri "$Base/$Asset" -OutFile (Join-Path $Tmp $Asset)
    Invoke-WebRequest -UseBasicParsing -Uri "$Base/$Asset.sha256" -OutFile (Join-Path $Tmp "$Asset.sha256")
    $want = ((Get-Content (Join-Path $Tmp "$Asset.sha256") -Raw).Trim() -split '\s+')[0].ToLower()
    $got = (Get-FileHash (Join-Path $Tmp $Asset) -Algorithm SHA256).Hash.ToLower()
    if ($want -ne $got) { throw "checksum mismatch for $Asset (expected $want, got $got)" }
    Write-Host 'Checksum verified.'
    Expand-Archive -Path (Join-Path $Tmp $Asset) -DestinationPath $Tmp -Force
    New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
    Copy-Item (Join-Path $Tmp 'agentlan.exe') (Join-Path $InstallDir 'agentlan.exe') -Force
}
finally {
    Remove-Item -Recurse -Force $Tmp -ErrorAction SilentlyContinue
}

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
Write-Host @'

Next steps:
  On the PC you pick as the server:   agentlan up
  On every other PC:                  agentlan connect <code printed by `agentlan up`>
  Add an agent on a PC:               agentlan agent add backend --role backend --runner claude-code
  Connect Claude Code to it:          claude mcp add -s local agentlan -- agentlan mcp --agent backend --runner claude-code

If this PC is the server, allow agentlan through Windows Firewall when prompted (TCP 47820, Private networks).
Docs: https://github.com/jenn0pal/agentlan
'@
