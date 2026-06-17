$ErrorActionPreference = "Stop"
Set-Location "D:\VSC Programs\FratFinderAI"
Get-Content .env | ForEach-Object {
  $line = $_.Trim()
  if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) { return }
  $idx = $line.IndexOf("=")
  $key = $line.Substring(0, $idx).Trim()
  $value = $line.Substring($idx + 1).Trim().Trim('"')
  if ($key) { [Environment]::SetEnvironmentVariable($key, $value, "Process") }
}
$env:WEB_AUTH_DISABLED = "true"
$env:NODE_ENV = "development"
$env:APP_ENV = "development"
$env:CRAWLER_SEARCH_SEARXNG_BASE_URL = "http://localhost:8889"
$env:CRAWLER_SEARCH_SEARXNG_BASE_URLS = "http://localhost:8889"
pnpm.cmd --filter @fratfinder/web dev -- --hostname 127.0.0.1 --port 3000
