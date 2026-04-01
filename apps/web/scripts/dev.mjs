import { existsSync, rmSync } from "fs";
import path from "path";
import { spawn } from "child_process";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const appDir = path.resolve(__dirname, "..");
const nextDir = path.join(appDir, ".next");

// Local operator work is more important than hot-reload cache reuse.
// Clean starts avoid stale route bundle lookups and broken _next asset paths.
if (existsSync(nextDir)) {
  rmSync(nextDir, { recursive: true, force: true });
}

const child = spawn("next", ["dev"], {
  cwd: appDir,
  stdio: "inherit",
  shell: process.platform === "win32",
  env: {
    ...process.env,
    FRATFINDER_WEB_DEV_CLEAN_START: "true"
  }
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 0);
});
