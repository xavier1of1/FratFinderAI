param(
  [switch]$SkipDocker,
  [switch]$FailOnHigh
)

$ErrorActionPreference = "Stop"

function Invoke-Native {
  param(
    [Parameter(Mandatory = $true)][string]$Command,
    [Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments
  )

  & $Command @Arguments
  if ($LASTEXITCODE -ne 0) {
    throw "$Command failed with exit code $LASTEXITCODE"
  }
}

function Invoke-NativeCapture {
  param(
    [Parameter(Mandatory = $true)][string]$OutputPath,
    [Parameter(Mandatory = $true)][string]$Command,
    [Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments
  )

  & $Command @Arguments | Out-File -Encoding utf8 $OutputPath
  if ($LASTEXITCODE -ne 0) {
    throw "$Command failed with exit code $LASTEXITCODE"
  }
}

function Require-Command($Name) {
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "$Name is required. Install it locally or run the GitHub Actions security-sbom-sca job."
  }
}

Require-Command "syft"
Require-Command "grype"

New-Item -ItemType Directory -Force -Path "sbom" | Out-Null
New-Item -ItemType Directory -Force -Path "security" | Out-Null
$webSbomContext = ".tmp/security-sbom-context/apps-web"
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $webSbomContext
New-Item -ItemType Directory -Force -Path "$webSbomContext/apps/web", "$webSbomContext/packages/contracts" | Out-Null
Copy-Item package.json, pnpm-lock.yaml, pnpm-workspace.yaml "$webSbomContext"
Copy-Item apps/web/package.json "$webSbomContext/apps/web/package.json"
Copy-Item packages/contracts/package.json "$webSbomContext/packages/contracts/package.json"
Remove-Item -Force -ErrorAction SilentlyContinue `
  "sbom/apps-web.cdx.json", `
  "sbom/crawler-python.cdx.json", `
  "sbom/docker-web.cdx.json", `
  "sbom/docker-crawler.cdx.json", `
  "security/grype-apps-web.json", `
  "security/grype-crawler-python.json", `
  "security/grype-docker-web.json", `
  "security/grype-docker-crawler.json", `
  "security/vulnerability-scan.json", `
  "security/sbom-evidence.md"

Invoke-Native python "scripts/security/security_sca.py" "validate-ignores" "--ignore-file" ".security/vulnerability-ignores.yml"

Invoke-Native syft "dir:$webSbomContext" "-o" "cyclonedx-json=sbom/apps-web.cdx.json"
Invoke-Native syft "dir:services/crawler" "-o" "cyclonedx-json=sbom/crawler-python.cdx.json"

if (-not $SkipDocker) {
  Invoke-Native docker "build" "-t" "fratfinder-web-sbom" "-f" "infra/docker/Dockerfile.web" "."
  Invoke-Native docker "build" "-t" "fratfinder-crawler-sbom" "-f" "infra/docker/Dockerfile.crawler" "."
  Invoke-Native syft "fratfinder-web-sbom" "-o" "cyclonedx-json=sbom/docker-web.cdx.json"
  Invoke-Native syft "fratfinder-crawler-sbom" "-o" "cyclonedx-json=sbom/docker-crawler.cdx.json"
}

Invoke-NativeCapture "security/grype-apps-web.json" grype "sbom:sbom/apps-web.cdx.json" "-o" "json"
Invoke-NativeCapture "security/grype-crawler-python.json" grype "sbom:sbom/crawler-python.cdx.json" "-o" "json"
if (-not $SkipDocker) {
  Invoke-NativeCapture "security/grype-docker-web.json" grype "sbom:sbom/docker-web.cdx.json" "-o" "json"
  Invoke-NativeCapture "security/grype-docker-crawler.json" grype "sbom:sbom/docker-crawler.cdx.json" "-o" "json"
}

Invoke-Native python "scripts/security/security_sca.py" "combine-scans" "--output" "security/vulnerability-scan.json" "security/grype-*.json"

$failFlag = @()
if ($FailOnHigh) {
  $failFlag = @("--fail-on-high")
}

$sboms = @("--sbom", "sbom/apps-web.cdx.json", "--sbom", "sbom/crawler-python.cdx.json")
if (-not $SkipDocker) {
  $sboms += @("--sbom", "sbom/docker-web.cdx.json", "--sbom", "sbom/docker-crawler.cdx.json")
}

Invoke-Native python "scripts/security/security_sca.py" "render-evidence" `
  "--ignore-file" ".security/vulnerability-ignores.yml" `
  "--scan" "security/vulnerability-scan.json" `
  @sboms `
  "--output" "security/sbom-evidence.md" `
  "--commit-sha" "local" `
  "--workflow-run-id" "local" `
  "--syft-version" "$(syft version | Select-Object -First 1)" `
  "--scanner-version" "$(grype version | Select-Object -First 1)"

Invoke-Native python "scripts/security/security_sca.py" "evaluate" "--ignore-file" ".security/vulnerability-ignores.yml" "--scan" "security/vulnerability-scan.json" @failFlag
