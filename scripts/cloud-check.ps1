param([string]$Profile = 'ephemeraldb-dev', [string]$Region = 'ap-south-1')
$ErrorActionPreference = 'Stop'
& aws sts get-caller-identity --profile $Profile --region $Region
if ($LASTEXITCODE -ne 0) { throw 'AWS session is unavailable. Reauthenticate this profile before live checks.' }
$env:AWS_PROFILE = $Profile
$env:AWS_REGION = $Region
if (-not $env:S3_EXPRESS_BUCKET -or -not $env:S3_CHECKPOINT_BUCKET) { throw 'Set both existing S3 bucket names before integration tests.' }
$env:RUN_AWS_TESTS = '1'
try {
    & python -m pytest -m aws -q
    if ($LASTEXITCODE -ne 0) { throw 'AWS integration checks failed; retain failed-run evidence.' }
} finally { Remove-Item Env:RUN_AWS_TESTS -ErrorAction SilentlyContinue }
