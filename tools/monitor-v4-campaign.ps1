param(
  [Parameter(Mandatory = $true)]
  [string]$CampaignId,

  [string]$ApiBaseUrl = "http://localhost:3200",

  [int]$PollSeconds = 60,

  [int]$InactivityAlertMinutes = 15,

  [string]$OutputDir = "logs/campaign-watch",

  [switch]$AutoCancelOnCritical
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

function Write-Line {
  param(
    [string]$Path,
    [string]$Message
  )

  $timestamp = (Get-Date).ToString("s")
  Add-Content -Path $Path -Value "[$timestamp] $Message"
}

function Get-Json {
  param([string]$Uri)
  return Invoke-RestMethod -Uri $Uri -Method Get -TimeoutSec 30
}

function Post-Json {
  param([string]$Uri)
  return Invoke-RestMethod -Uri $Uri -Method Post -ContentType "application/json" -Body "{}" -TimeoutSec 30
}

if (-not (Test-Path $OutputDir)) {
  New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
}

$safeId = $CampaignId.Replace(":", "_")
$logPath = Join-Path $OutputDir "$safeId.log"
$latestPath = Join-Path $OutputDir "$safeId.latest.json"
$alertsPath = Join-Path $OutputDir "$safeId.alerts.log"

Write-Line -Path $logPath -Message "watchdog_started api=$ApiBaseUrl poll_seconds=$PollSeconds inactivity_minutes=$InactivityAlertMinutes auto_cancel=$($AutoCancelOnCritical.IsPresent)"

$lastEventCount = -1
$lastItemCount = -1
$lastProgressAt = Get-Date
$lastKnownStatus = ""

while ($true) {
  try {
    $response = Get-Json -Uri "$ApiBaseUrl/api/campaign-runs/$CampaignId"
    $data = $response.data

    if ($null -eq $data) {
      throw "Campaign payload missing data envelope"
    }

    $eventCount = @($data.events).Count
    $itemCount = @($data.items).Count
    $status = [string]$data.status
    $runtimeActive = [bool]$data.runtimeActive
    $lastError = ""
    if ($null -ne $data.lastError) {
      $lastError = [string]$data.lastError
    }
    $summary = $data.summary
    $telemetry = $data.telemetry
    $queueAlert = $telemetry.queueStallAlert

    if ($eventCount -ne $lastEventCount -or $itemCount -ne $lastItemCount -or $status -ne $lastKnownStatus) {
      $lastProgressAt = Get-Date
      $lastEventCount = $eventCount
      $lastItemCount = $itemCount
      $lastKnownStatus = $status
    }

    $snapshot = [ordered]@{
      capturedAt = (Get-Date).ToString("o")
      campaignId = $CampaignId
      status = $status
      runtimeActive = $runtimeActive
      lastError = $lastError
      itemCount = $itemCount
      eventCount = $eventCount
      completedCount = $summary.completedCount
      failedCount = $summary.failedCount
      skippedCount = $summary.skippedCount
      activeCount = $summary.activeCount
      jobsPerMinute = $summary.jobsPerMinute
      queueDepthStart = $summary.queueDepthStart
      queueDepthEnd = $summary.queueDepthEnd
      queueDepthDelta = $summary.queueDepthDelta
      totalProcessed = $summary.totalProcessed
      totalRequeued = $summary.totalRequeued
      checkpointCount = $summary.checkpointCount
      activePolicyVersion = $telemetry.activePolicyVersion
      activePolicySnapshotId = $telemetry.activePolicySnapshotId
      delayedRewardHealth = $telemetry.delayedRewardHealth
      queueStallAlert = $queueAlert
      latestEventTypes = @($data.events | Select-Object -Last 5 | ForEach-Object { $_.eventType })
    }

    $snapshot | ConvertTo-Json -Depth 8 | Set-Content -Path $latestPath
    Write-Line -Path $logPath -Message ("status={0} runtime={1} items={2} events={3} completed={4} failed={5} jobs_per_minute={6}" -f $status, $runtimeActive, $itemCount, $eventCount, $summary.completedCount, $summary.failedCount, $summary.jobsPerMinute)

    $minutesSinceProgress = ((Get-Date) - $lastProgressAt).TotalMinutes
    $criticalReasons = New-Object System.Collections.Generic.List[string]

    if ($status -eq "failed") {
      $criticalReasons.Add("campaign_failed")
    }
    if ($status -eq "canceled") {
      $criticalReasons.Add("campaign_canceled")
    }
    if ($status -eq "running" -and -not $runtimeActive) {
      $criticalReasons.Add("runtime_inactive_while_running")
    }
    if ($lastError) {
      $criticalReasons.Add("last_error:$lastError")
    }
    if ($queueAlert) {
      $criticalReasons.Add("queue_stall_alert")
    }
    if ($minutesSinceProgress -ge $InactivityAlertMinutes -and $status -eq "running") {
      $criticalReasons.Add(("no_progress_{0}m" -f [int][Math]::Round($minutesSinceProgress)))
    }

    if ($criticalReasons.Count -gt 0) {
      $reasonText = ($criticalReasons -join ", ")
      Write-Line -Path $alertsPath -Message ("CRITICAL {0}" -f $reasonText)

      if ($AutoCancelOnCritical.IsPresent -and $status -eq "running") {
        try {
          Post-Json -Uri "$ApiBaseUrl/api/campaign-runs/$CampaignId/cancel" | Out-Null
          Write-Line -Path $alertsPath -Message "ACTION canceled_campaign_due_to_critical_condition"
        } catch {
          Write-Line -Path $alertsPath -Message ("ACTION_FAILED cancel_error={0}" -f $_.Exception.Message)
        }
      }

      if ($status -in @("failed", "canceled", "completed")) {
        Write-Line -Path $logPath -Message "watchdog_stopped_terminal_campaign"
        break
      }
    }
  } catch {
    Write-Line -Path $alertsPath -Message ("MONITOR_ERROR {0}" -f $_.Exception.Message)
  }

  Start-Sleep -Seconds $PollSeconds
}
