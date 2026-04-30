Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$outputZip = Join-Path $projectRoot "dgp-discloud-upload.zip"
$stagingDir = Join-Path $projectRoot ".discloud-build"

$includePaths = @(
    ".env",
    "discloud.config",
    "main.py",
    "app.py",
    "wsgi.py",
    "passenger_wsgi.py",
    "requirements.txt",
    "README.md",
    ".env.discloud.example",
    "python_app",
    "data"
)

if (Test-Path -LiteralPath $stagingDir) {
    Remove-Item -LiteralPath $stagingDir -Recurse -Force
}

if (Test-Path -LiteralPath $outputZip) {
    Remove-Item -LiteralPath $outputZip -Force
}

New-Item -ItemType Directory -Path $stagingDir | Out-Null

foreach ($relativePath in $includePaths) {
    $sourcePath = Join-Path $projectRoot $relativePath
    if (-not (Test-Path -LiteralPath $sourcePath)) {
        continue
    }

    $destinationPath = Join-Path $stagingDir $relativePath
    $destinationParent = Split-Path -Parent $destinationPath
    if (-not (Test-Path -LiteralPath $destinationParent)) {
        New-Item -ItemType Directory -Path $destinationParent -Force | Out-Null
    }

    $item = Get-Item -LiteralPath $sourcePath
    if ($item.PSIsContainer) {
        Copy-Item -LiteralPath $sourcePath -Destination $destinationPath -Recurse -Force
    } else {
        Copy-Item -LiteralPath $sourcePath -Destination $destinationPath -Force
    }
}

$cacheDirectories = Get-ChildItem -LiteralPath $stagingDir -Recurse -Directory -Force |
    Where-Object { $_.Name -in @("__pycache__", ".git", ".venv", "python-runtime") } |
    Sort-Object FullName -Descending

foreach ($cacheDirectory in $cacheDirectories) {
    Remove-Item -LiteralPath $cacheDirectory.FullName -Recurse -Force -ErrorAction SilentlyContinue
}

$cacheFiles = Get-ChildItem -LiteralPath $stagingDir -Recurse -File -Force |
    Where-Object { $_.Extension -eq ".pyc" }

foreach ($cacheFile in $cacheFiles) {
    Remove-Item -LiteralPath $cacheFile.FullName -Force -ErrorAction SilentlyContinue
}

Compress-Archive -Path (Join-Path $stagingDir "*") -DestinationPath $outputZip -Force

Write-Host ""
Write-Host "Pacote criado:"
Write-Host $outputZip
Write-Host ""
Write-Host "Envie esse arquivo para a Discloud."
