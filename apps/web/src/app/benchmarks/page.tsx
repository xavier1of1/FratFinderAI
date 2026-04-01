import { BenchmarksDashboard } from "@/components/benchmarks-dashboard";
import { PageIntro } from "@/components/page-intro";
import { fetchFromApi } from "@/lib/api-client";
import type { BenchmarkRunListItem, CampaignRun, CrawlRunListItem } from "@/lib/types";

export default async function BenchmarksPage() {
  const [benchmarks, campaigns, runs] = await Promise.all([
    fetchFromApi<BenchmarkRunListItem[]>("/api/benchmarks?limit=200"),
    fetchFromApi<CampaignRun[]>("/api/campaign-runs?limit=50"),
    fetchFromApi<CrawlRunListItem[]>("/api/runs?limit=600")
  ]);
  const running = benchmarks.filter((item) => item.status === "running" || item.status === "queued").length;
  const latest = benchmarks[0] ?? null;
  const activeCampaigns = campaigns.filter((item) => item.status === "running" || item.status === "queued").length;

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Benchmarks"
        title="System performance and throughput benchmarks"
        description="Launch configurable benchmark runs, compare saved results, inspect per-cycle queue behavior, and pivot into longer campaign validations when you need broader proof."
        meta={[
          `${benchmarks.length} saved runs`,
          `${running} active`,
          `${activeCampaigns} active campaigns`,
          latest ? `latest: ${latest.name}` : "no benchmarks yet"
        ]}
      />
      <BenchmarksDashboard initialBenchmarks={benchmarks} initialRuns={runs} activeCampaignCount={activeCampaigns} />
    </div>
  );
}


