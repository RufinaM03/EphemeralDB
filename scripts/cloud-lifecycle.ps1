param([Parameter(Mandatory)][ValidateSet('plan','apply','destroy-plan','destroy','inventory')][string]$Action,
      [string]$Profile = 'ephemeraldb-dev', [string]$Region = 'ap-south-1')
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$env:AWS_PROFILE = $Profile
$env:AWS_REGION = $Region
if ($Action -eq 'inventory') {
    & python (Join-Path $PSScriptRoot 'aws_inventory.py') --profile $Profile --region $Region
    exit $LASTEXITCODE
}
Push-Location -LiteralPath (Join-Path $projectRoot 'terraform')
try {
    & terraform init
    if ($LASTEXITCODE -ne 0) { throw 'Terraform initialization failed' }
    switch ($Action) {
        'plan' { & terraform plan -out=reviewed.tfplan }
        'apply' {
            if (-not (Test-Path -LiteralPath 'reviewed.tfplan')) { throw 'Create and review a saved plan first.' }
            & terraform apply reviewed.tfplan
        }
        'destroy-plan' { & terraform plan -destroy -out=teardown.tfplan }
        'destroy' {
            if (-not (Test-Path -LiteralPath 'teardown.tfplan')) { throw 'Create and review a teardown plan with -Action destroy-plan first.' }
            & terraform apply teardown.tfplan
        }
    }
    if ($LASTEXITCODE -ne 0) { throw 'Terraform operation failed' }
} finally { Pop-Location }
