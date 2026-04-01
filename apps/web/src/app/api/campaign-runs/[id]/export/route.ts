import { buildCampaignReport } from "@/lib/campaign-report";
import { toApiErrorResponse } from "@/lib/api-envelope";
import { getCampaignRun } from "@/lib/repositories/campaign-run-repository";

function toCsv(report: ReturnType<typeof buildCampaignReport>): string {
  const lines: string[] = [];
  lines.push("section,key,value");
  lines.push(`summary,campaign_name,\"${report.campaignName.replace(/\"/g, '""')}\"`);
  lines.push(`summary,campaign_id,${report.campaignId}`);
  lines.push(`summary,generated_at,${report.generatedAt}`);
  for (const [key, value] of Object.entries(report.summary)) {
    lines.push(`summary,${key},${value}`);
  }
  for (const cohort of report.cohortComparison) {
    for (const [key, value] of Object.entries(cohort)) {
      lines.push(`cohort_${cohort.cohort},${key},${value}`);
    }
  }
  for (const failure of report.topFailureReasons) {
    lines.push(`top_failure,\"${failure.reason.replace(/\"/g, '""')}\",${failure.count}`);
  }
  for (const habit of report.topSuccessfulHabits) {
    lines.push(`successful_habit,${habit.label},${habit.value}`);
  }
  for (const recommendation of report.recommendations) {
    lines.push(`recommendation,\"${recommendation.replace(/\"/g, '""')}\",1`);
  }
  for (const point of report.providerHealthHistory) {
    lines.push(`provider_health,${point.timestamp},${point.successRate}`);
  }
  return lines.join("\n");
}

export async function GET(request: Request, context: { params: { id: string } }) {
  try {
    const run = await getCampaignRun(context.params.id);
    if (!run) {
      throw new Error(`Campaign run ${context.params.id} not found`);
    }

    const report = buildCampaignReport(run);
    const { searchParams } = new URL(request.url);
    const format = searchParams.get("format") ?? "json";

    if (format === "csv") {
      return new Response(toCsv(report), {
        status: 200,
        headers: {
          "Content-Type": "text/csv; charset=utf-8",
          "Content-Disposition": `attachment; filename=campaign-${run.id}.csv`
        }
      });
    }

    return new Response(JSON.stringify(report, null, 2), {
      status: 200,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Content-Disposition": `attachment; filename=campaign-${run.id}.json`
      }
    });
  } catch (error) {
    return toApiErrorResponse(error);
  }
}
