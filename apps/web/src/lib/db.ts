import { existsSync, readFileSync } from "fs";
import path from "path";
import { Pool } from "pg";

declare global {
  // eslint-disable-next-line no-var
  var __fratfinderPool: Pool | undefined;
  // eslint-disable-next-line no-var
  var __fratfinderEnvLoaded: boolean | undefined;
}

function loadEnvFile(filePath: string): void {
  if (!existsSync(filePath)) {
    return;
  }

  const lines = readFileSync(filePath, "utf-8").split(/\r?\n/);
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) {
      continue;
    }

    const separatorIndex = line.indexOf("=");
    if (separatorIndex <= 0) {
      continue;
    }

    const key = line.slice(0, separatorIndex).trim();
    if (!key || process.env[key] !== undefined) {
      continue;
    }

    let value = line.slice(separatorIndex + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }

    process.env[key] = value;
  }
}

function ensureDatabaseUrl(): string {
  if (process.env.DATABASE_URL) {
    return process.env.DATABASE_URL;
  }

  if (!global.__fratfinderEnvLoaded) {
    const projectDir = process.cwd();
    const repoRoot = path.resolve(projectDir, "../..");

    for (const baseDir of [projectDir, repoRoot]) {
      loadEnvFile(path.join(baseDir, ".env.local"));
      loadEnvFile(path.join(baseDir, ".env"));
    }

    global.__fratfinderEnvLoaded = true;
  }

  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is not set");
  }

  return databaseUrl;
}

export function getDbPool(): Pool {
  const databaseUrl = ensureDatabaseUrl();

  if (!global.__fratfinderPool) {
    global.__fratfinderPool = new Pool({
      connectionString: databaseUrl,
      max: 10
    });
  }

  return global.__fratfinderPool;
}
