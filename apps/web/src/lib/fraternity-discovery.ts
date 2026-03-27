import { spawn } from "child_process";
import { existsSync } from "fs";
import path from "path";

import type { FraternityDiscoveryCandidate } from "@/lib/types";

export interface FraternitySourceDiscoveryResult {
  fraternityName: string;
  fraternitySlug: string;
  selectedUrl: string | null;
  selectedConfidence: number;
  confidenceTier: "high" | "medium" | "low";
  candidates: FraternityDiscoveryCandidate[];
}

function findRepositoryRoot(): string {
  let currentDir = process.cwd();
  for (let index = 0; index < 6; index += 1) {
    if (existsSync(path.join(currentDir, "pnpm-workspace.yaml"))) {
      return currentDir;
    }
    const parentDir = path.dirname(currentDir);
    if (parentDir === currentDir) {
      break;
    }
    currentDir = parentDir;
  }
  return process.cwd();
}

function parseDiscoveryOutput(output: string): FraternitySourceDiscoveryResult {
  const start = output.indexOf("{");
  const end = output.lastIndexOf("}");
  if (start < 0 || end <= start) {
    throw new Error("Could not parse discover-source output");
  }

  const payload = JSON.parse(output.slice(start, end + 1)) as {
    fraternity_name: string;
    fraternity_slug: string;
    selected_url: string | null;
    selected_confidence: number;
    confidence_tier: "high" | "medium" | "low";
    candidates: FraternityDiscoveryCandidate[];
  };

  return {
    fraternityName: payload.fraternity_name,
    fraternitySlug: payload.fraternity_slug,
    selectedUrl: payload.selected_url,
    selectedConfidence: Number(payload.selected_confidence ?? 0),
    confidenceTier: payload.confidence_tier,
    candidates: Array.isArray(payload.candidates) ? payload.candidates : []
  };
}

export async function discoverFraternitySource(fraternityName: string): Promise<FraternitySourceDiscoveryResult> {
  const args = ["-m", "fratfinder_crawler.cli", "discover-source", "--fraternity-name", fraternityName];
  const workingDirectory = findRepositoryRoot();

  const output = await new Promise<string>((resolve, reject) => {
    const child = spawn("python", args, {
      cwd: workingDirectory,
      env: process.env,
      windowsHide: true
    });

    let stdout = "";
    let stderr = "";
    let settled = false;

    const settle = (callback: (value: string | Error) => void, value: string | Error) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      callback(value);
    };

    const timeout = setTimeout(() => {
      if (!child.killed) {
        if (process.platform === "win32" && child.pid) {
          spawn("taskkill", ["/PID", String(child.pid), "/T", "/F"], {
            windowsHide: true,
            stdio: "ignore"
          }).unref();
        } else {
          child.kill("SIGKILL");
        }
      }
      settle((value) => reject(value as Error), new Error("discover-source command timed out"));
    }, 90_000);

    child.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString("utf-8");
    });

    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf-8");
    });

    child.on("error", (error) => {
      settle((value) => reject(value as Error), error);
    });

    child.on("close", (code) => {
      if (code !== 0) {
        settle((value) => reject(value as Error), new Error(`discover-source failed with code ${code}: ${stderr || stdout}`));
        return;
      }
      settle((value) => resolve(value as string), `${stdout}\n${stderr}`);
    });
  });

  return parseDiscoveryOutput(output);
}
