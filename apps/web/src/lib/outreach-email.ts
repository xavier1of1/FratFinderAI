import { execFile } from "child_process";
import { promisify } from "util";

import type { CrmDispatchMode } from "./types";

const execFileAsync = promisify(execFile);

export interface OutlookDeliveryPayload {
  to: string;
  subject: string;
  body: string;
  mode: CrmDispatchMode;
}

function buildPowerShellScript(): string {
  return `
$payloadBase64 = [string]$env:CRM_OUTLOOK_PAYLOAD_B64
if ([string]::IsNullOrWhiteSpace($payloadBase64)) {
  throw 'Missing CRM_OUTLOOK_PAYLOAD_B64 payload.'
}
$payloadJson = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($payloadBase64))
$payload = $payloadJson | ConvertFrom-Json
$outlook = New-Object -ComObject Outlook.Application
$mail = $outlook.CreateItem(0)
$mail.To = [string]$payload.to
$mail.Subject = [string]$payload.subject
$mail.Body = [string]$payload.body
if ([string]$payload.mode -eq 'send') {
  $mail.Send()
  $result = @{ success = $true; action = 'sent' }
} else {
  $mail.Save()
  $result = @{ success = $true; action = 'drafted' }
}
[System.Runtime.InteropServices.Marshal]::ReleaseComObject($mail) | Out-Null
[System.Runtime.InteropServices.Marshal]::ReleaseComObject($outlook) | Out-Null
$result | ConvertTo-Json -Compress
`.trim();
}

export async function deliverOutlookMessage(payload: OutlookDeliveryPayload): Promise<{
  success: true;
  action: "drafted" | "sent";
}> {
  const encodedPayload = Buffer.from(JSON.stringify(payload), "utf8").toString("base64");
  const { stdout, stderr } = await execFileAsync(
    "powershell.exe",
    [
      "-NoProfile",
      "-NonInteractive",
      "-ExecutionPolicy",
      "Bypass",
      "-Command",
      buildPowerShellScript()
    ],
    {
      env: {
        ...process.env,
        CRM_OUTLOOK_PAYLOAD_B64: encodedPayload
      },
      windowsHide: true,
      maxBuffer: 1024 * 1024
    }
  );

  if (stderr?.trim()) {
    throw new Error(stderr.trim());
  }

  const parsed = JSON.parse(stdout.trim()) as { success: boolean; action: "drafted" | "sent" };
  if (!parsed.success) {
    throw new Error("Outlook delivery failed.");
  }

  return { success: true, action: parsed.action };
}
