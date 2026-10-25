param(
    [ValidateSet('run', 'verify', 'doctor', 'down')]
    [string]$Action = 'run',
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$CliArgs
)
$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$PythonExe = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $PythonExe)) { $PythonExe = 'python' }
$PreviousPythonPath = $env:PYTHONPATH
try {
    $env:PYTHONPATH = Join-Path $ProjectRoot 'src'
    & $PythonExe -m freewaypulse.demo $Action @CliArgs
    $ResultCode = $LASTEXITCODE
} finally {
    $env:PYTHONPATH = $PreviousPythonPath
}
exit $ResultCode
