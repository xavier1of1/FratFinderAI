import { runEvaluationWorker } from "../src/lib/evaluation-worker";

function parseBoolean(value: string | undefined): boolean {
  if (!value) {
    return false;
  }
  const normalized = value.trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on";
}

async function main(): Promise<void> {
  const args = new Map<string, string>();
  for (let index = 2; index < process.argv.length; index += 2) {
    const key = process.argv[index];
    const value = process.argv[index + 1] ?? "true";
    args.set(key, value);
  }

  const result = await runEvaluationWorker({
    once: parseBoolean(args.get("--once")),
    pollMs: args.has("--poll-ms") ? Number(args.get("--poll-ms")) : undefined,
    limit: args.has("--limit") ? Number(args.get("--limit")) : undefined,
  });
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

void main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`);
  process.exitCode = 1;
});
