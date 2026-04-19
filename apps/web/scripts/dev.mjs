import { existsSync, rmSync } from "fs";
import path from "path";
import { spawn } from "child_process";
import { fileURLToPath } from "url";
import { createRequire } from "module";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const appDir = path.resolve(__dirname, "..");
const runtimeDistDir = `.next-dev-${process.pid}`;
const nextDir = path.join(appDir, runtimeDistDir);
const require = createRequire(import.meta.url);
const nextCliPath = require.resolve("next/dist/bin/next");
const forwardedArgs = process.argv.slice(2).filter((arg, index, allArgs) => !(arg === "--" && index === 0));

// Local operator work is more important than hot-reload cache reuse.
// Clean starts avoid stale route bundle lookups and broken _next asset paths.
if (existsSync(nextDir)) {
  try {
    rmSync(nextDir, { recursive: true, force: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(`[fratfinder-web] clean start skipped for .next: ${message}`);
  }
}

const child = spawn(process.execPath, [nextCliPath, "dev", ...forwardedArgs], {
  cwd: appDir,
  stdio: "inherit",
  env: {
    ...process.env,
    FRATFINDER_WEB_DEV_CLEAN_START: "true",
    FRATFINDER_WEB_DIST_DIR: runtimeDistDir
  }
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 0);
});
