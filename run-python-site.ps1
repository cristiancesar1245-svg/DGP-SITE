Set-Location -LiteralPath $PSScriptRoot

$port = 8080
if (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) {
    $port = 8090
}
$env:DGP_PORT = "$port"
$env:DGP_DEV_RELOAD = "1"
$url = "http://127.0.0.1:$port"
$openUrl = "$url/login"

function Get-PreferredIPv4Address {
    param([string[]] $InterfaceAliases)

    foreach ($alias in $InterfaceAliases) {
        $address = Get-NetIPAddress -AddressFamily IPv4 -InterfaceAlias $alias -ErrorAction SilentlyContinue |
            Where-Object { $_.IPAddress -and $_.IPAddress -notlike '169.254.*' } |
            Select-Object -First 1 -ExpandProperty IPAddress
        if ($address) {
            return $address
        }
    }

    return $null
}

$radminIp = Get-PreferredIPv4Address -InterfaceAliases @("Radmin VPN")
$wifiIp = Get-PreferredIPv4Address -InterfaceAliases @("Wi-Fi", "Ethernet")
$remoteUrls = @()

if ($radminIp) {
    $remoteUrls += "Radmin VPN: http://${radminIp}:$port/login"
}

if ($wifiIp) {
    $remoteUrls += "Rede local: http://${wifiIp}:$port/login"
}

Write-Host "Reiniciando o sistema DGP na porta $port..."

function Get-ListeningProcessIds {
    param([int] $LocalPort)

    $connections = Get-NetTCPConnection -LocalPort $LocalPort -State Listen -ErrorAction SilentlyContinue
    @($connections | Select-Object -ExpandProperty OwningProcess -Unique | Where-Object { $_ -and $_ -ne 0 })
}

function Get-ProjectPythonProcessIds {
    $projectPathPattern = [regex]::Escape($PSScriptRoot)
    @(
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object {
                $_.CommandLine -and
                $_.CommandLine -match $projectPathPattern -and
                (
                    $_.CommandLine -match 'python_app\\app\.py' -or
                    $_.CommandLine -match '\bmain\.py\b'
                )
            } |
            Select-Object -ExpandProperty ProcessId -Unique |
            Where-Object { $_ -and $_ -ne 0 }
    )
}

function Get-DescendantProcessIds {
    param([int] $ProcessId)

    $childProcessIds = @(
        Get-CimInstance Win32_Process -Filter "ParentProcessId = $ProcessId" -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty ProcessId
    )

    foreach ($childProcessId in $childProcessIds) {
        $childProcessId
        Get-DescendantProcessIds -ProcessId $childProcessId
    }
}

function Stop-ProcessTree {
    param([int] $ProcessId)

    $processIdsToStop = @($ProcessId) + @(Get-DescendantProcessIds -ProcessId $ProcessId)
    foreach ($processIdToStop in ($processIdsToStop | Sort-Object -Descending -Unique)) {
        $process = Get-Process -Id $processIdToStop -ErrorAction SilentlyContinue
        if ($process) {
            Write-Host "Encerrando servidor antigo na porta $port - PID $processIdToStop ($($process.ProcessName))"
            Stop-Process -Id $processIdToStop -Force -ErrorAction SilentlyContinue
        }
    }
}

$processIds = @(Get-ListeningProcessIds -LocalPort $port) + @(Get-ProjectPythonProcessIds)
foreach ($processId in $processIds) {
    Stop-ProcessTree -ProcessId $processId
}

for ($attempt = 1; $attempt -le 30; $attempt++) {
    Start-Sleep -Milliseconds 200
    if (-not (Get-ListeningProcessIds -LocalPort $port)) {
        break
    }
}

if (Get-ListeningProcessIds -LocalPort $port) {
    Write-Error "A porta $port continua ocupada. Feche o processo manualmente e tente novamente."
    exit 1
}

$venvPython = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (Test-Path -LiteralPath $venvPython) {
    $pythonFile = $venvPython
    $pythonArgs = @("-u", "main.py")
} else {
    $pythonCommand = Get-Command py -ErrorAction SilentlyContinue
    if ($pythonCommand) {
        $pythonFile = $pythonCommand.Source
        $pythonArgs = @("-3", "-u", "main.py")
    } else {
        $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
        if (-not $pythonCommand) {
            Write-Error "Python nao foi encontrado. Instale o Python ou ajuste o PATH."
            exit 1
        }
        $pythonFile = $pythonCommand.Source
        $pythonArgs = @("-u", "main.py")
    }
}

Write-Host ""
Write-Host "Iniciando o sistema em $url"
if ($remoteUrls) {
    Write-Host "Acesso remoto disponivel em:"
    foreach ($remoteUrl in $remoteUrls) {
        Write-Host " - $remoteUrl"
    }
}
Write-Host "O servidor sera aberto em uma nova janela."
Write-Host ""

Start-Process -FilePath $pythonFile -ArgumentList $pythonArgs -WorkingDirectory $PSScriptRoot -WindowStyle Normal

for ($attempt = 1; $attempt -le 50; $attempt++) {
    Start-Sleep -Milliseconds 200
    if (Get-ListeningProcessIds -LocalPort $port) {
        Write-Host "Sistema iniciado em $url"
        Start-Process $openUrl
        exit 0
    }
}

Write-Error "O servidor foi iniciado, mas nao respondeu na porta $port. Confira a janela do Python para ver o erro."
exit 1
