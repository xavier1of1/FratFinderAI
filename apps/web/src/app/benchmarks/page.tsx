import { BenchmarksDashboard } from "@/components/benchmarks-dashboard";
import { PageIntro } from "@/components/page-intro";
import { getBenchmarkRunCounts, listBenchmarkRuns } from "@/lib/repositories/benchmark-repository";
import { listCampaignRuns } from "@/lib/repositories/campaign-run-repository";
import { listCrawlRuns } from "@/lib/repositories/crawl-run-repository";
import type { BenchmarkRunListItem, CampaignRun, CrawlRunListItem } from "@/lib/types";

export const dynamic = "force-dynamic";

export default async function BenchmarksPage() {
  const [benchmarks, campaigns, runs, counts] = await Promise.all([
    listBenchmarkRuns(200) as Promise<BenchmarkRunListItem[]>,
    listCampaignRuns(50) as Promise<CampaignRun[]>,
    listCrawlRuns(600) as Promise<CrawlRunListItem[]>,
    getBenchmarkRunCounts()
  ]);
  const running = counts.running + counts.queued;
  const latest = benchmarks[0] ?? null;
  const activeCampaigns = campaigns.filter((item) => item.status === "running" || item.status === "queued").length;

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Benchmarks"
        title="System performance and throughput benchmarks"
        description="Launch configurable benchmark runs, compare saved results, inspect per-cycle queue behavior, and pivot into longer campaign validations when you need broader proof."
        meta={[
          `${counts.total} saved runs`,
          `${running} active`,
          `${activeCampaigns} active campaigns`,
          latest ? `latest: ${latest.name}` : "no benchmarks yet"
        ]}
      />
      <BenchmarksDashboard initialBenchmarks={benchmarks} initialRuns={runs} activeCampaignCount={activeCampaigns} summaryCounts={counts} />
    </div>
  );
}


