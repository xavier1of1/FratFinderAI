import { buildBenchmarkGateReport, findLatestLegacyBaseline, renderBenchmarkGateMarkdown } from "@/lib/benchmark-gates";
import { apiError, toApiErrorResponse } from "@/lib/api-envelope";
import { getBenchmarkRun, listBenchmarkRuns } from "@/lib/repositories/benchmark-repository";

export const dynamic = "force-dynamic";

export async function GET(request: Request, context: { params: { id: string } }) {
  try {
    const run = await getBenchmarkRun(context.params.id);
    if (!run) {
      return apiError({ status: 404, code: "not_found", message: `Benchmark ${context.params.id} not found` });
    }

    const allRuns = await listBenchmarkRuns(500);
    const baseline = findLatestLegacyBaseline(allRuns, run);
    const gateReport = buildBenchmarkGateReport(run, baseline);

    const url = new URL(request.url);
    const format = String(url.searchParams.get("format") ?? "json").trim().toLowerCase();

    if (format === "md" || format === "markdown") {
      const markdown = renderBenchmarkGateMarkdown({ run, baseline, gateReport });
      return new Response(markdown, {
        status: 200,
        headers: {
          "Content-Type": "text/markdown; charset=utf-8",
          "Content-Disposition": `attachment; filename=benchmark-${run.id}-gates.md`,
        },
      });
    }

    const payload = {
      run,
      baseline,
      gateReport,
      exportedAt: new Date().toISOString(),
    };

    return new Response(JSON.stringify(payload, null, 2), {
      status: 200,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Content-Disposition": `attachment; filename=benchmark-${run.id}-gates.json`,
      },
    });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
