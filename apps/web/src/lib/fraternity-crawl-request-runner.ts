import { spawn } from "child_process";
import { existsSync } from "fs";
import path from "path";

import {
  appendFraternityCrawlRequestEvent,
  getFraternityCrawlRequest,
  listDueQueuedFraternityCrawlRequestIds,
  reconcileStaleFraternityCrawlRequests,
  updateFraternityCrawlRequest,
} from "@/lib/repositories/fraternity-crawl-request-repository";

const DEFAULT_CRAWL_RUNTIME_MODE = "adaptive_assisted";
const DEFAULT_FIELD_JOB_RUNTIME_MODE = "langgraph_primary";
const DEFAULT_FIELD_JOB_GRAPH_DURABILITY = "sync";
const DEFAULT_LOCAL_REQUEST_TRIGGER_ENABLED = (() => {
  const appEnv = String(process.env.APP_ENV ?? process.env.NODE_ENV ?? "development").trim().toLowerCase();
  return appEnv === "development" || appEnv === "test";
})();
const LOCAL_REQUEST_TRIGGER_ENABLED = process.env.CRAWLER_LOCAL_REQUEST_TRIGGER_ENABLED == null
  ? DEFAULT_LOCAL_REQUEST_TRIGGER_ENABLED
  : String(process.env.CRAWLER_LOCAL_REQUEST_TRIGGER_ENABLED).trim().toLowerCase() !== "false";
const activeRequestRuns = new Set<string>();

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

function buildPythonEnv(repositoryRoot: string): NodeJS.ProcessEnv {
  const crawlerSrc = path.join(repositoryRoot, "services", "crawler", "src");
  const existingPythonPath = process.env.PYTHONPATH?.trim();
  return {
    ...process.env,
    PYTHONPATH: existingPythonPath ? `${crawlerSrc}${path.delimiter}${existingPythonPath}` : crawlerSrc,
  };
}

async function executeFraternityCrawlRequest(requestId: string): Promise<void> {
  const request = await getFraternityCrawlRequest(requestId);
  if (!request) {
    return;
  }
  if (request.status !== "queued" && request.status !== "running") {
    return;
  }

  const repositoryRoot = findRepositoryRoot();
  const args = [
    "-m",
    "fratfinder_crawler.cli",
    "run-request",
    "--request-id",
    requestId,
    "--crawl-runtime-mode",
    DEFAULT_CRAWL_RUNTIME_MODE,
    "--field-job-runtime-mode",
    DEFAULT_FIELD_JOB_RUNTIME_MODE,
    "--graph-durability",
    DEFAULT_FIELD_JOB_GRAPH_DURABILITY,
  ];

  await appendFraternityCrawlRequestEvent({
    requestId,
    eventType: "python_supervisor_dispatch",
    message: "Dispatched request to the Python request supervisor",
    payload: {
      command: `python ${args.join(" ")}`,
      crawlRuntimeMode: DEFAULT_CRAWL_RUNTIME_MODE,
      fieldJobRuntimeMode: DEFAULT_FIELD_JOB_RUNTIME_MODE,
      graphDurability: DEFAULT_FIELD_JOB_GRAPH_DURABILITY,
    },
  });

  await new Promise<void>((resolve, reject) => {
    const child = spawn("python", args, {
      cwd: repositoryRoot,
      env: buildPythonEnv(repositoryRoot),
      windowsHide: true,
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString("utf-8");
    });

    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf-8");
    });

    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`python ${args.join(" ")} exited with code ${code}: ${stderr || stdout}`));
        return;
      }
      resolve();
    });
  });
}

export async function scheduleFraternityCrawlRequest(requestId: string): Promise<boolean> {
  if (!LOCAL_REQUEST_TRIGGER_ENABLED) {
    return false;
  }
  if (activeRequestRuns.has(requestId)) {
    return false;
  }

  activeRequestRuns.add(requestId);
  queueMicrotask(() => {
    void executeFraternityCrawlRequest(requestId)
      .catch(async (error) => {
        const message = error instanceof Error ? error.message : String(error);
        await updateFraternityCrawlRequest({
          id: requestId,
          status: "failed",
          stage: "failed",
          finishedAtNow: true,
          lastError: message,
        });
        await appendFraternityCrawlRequestEvent({
          requestId,
          eventType: "python_supervisor_failed",
          message: "Python request supervisor dispatch failed",
          payload: { error: message },
        });
      })
      .finally(() => {
        activeRequestRuns.delete(requestId);
      });
  });

  return true;
}

export async function scheduleDueFraternityCrawlRequests(limit = 20): Promise<number> {
  await reconcileStaleFraternityCrawlRequests();
  if (!LOCAL_REQUEST_TRIGGER_ENABLED) {
    return 0;
  }
  const dueIds = await listDueQueuedFraternityCrawlRequestIds(limit);
  let scheduled = 0;
  for (const id of dueIds) {
    const didSchedule = await scheduleFraternityCrawlRequest(id);
    if (didSchedule) {
      scheduled += 1;
    }
  }
  return scheduled;
}

export function isFraternityCrawlRequestActive(requestId: string): boolean {
  return activeRequestRuns.has(requestId);
}

export function activeFraternityCrawlRequestCount(): number {
  return activeRequestRuns.size;
}
