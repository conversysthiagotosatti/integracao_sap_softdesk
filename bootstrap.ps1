# Recria a venv local e instala as dependências do microsserviço (Windows).
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
if (-not (Test-Path ".\.venv\Scripts\python.exe")) {
    py -3 -m venv .venv
}
& ".\.venv\Scripts\python.exe" -m pip install --upgrade pip
& ".\.venv\Scripts\python.exe" -m pip install -r ".\sap_integration_service\requirements.txt"
Write-Host "Concluído. Ative a venv: .\.venv\Scripts\Activate.ps1"
