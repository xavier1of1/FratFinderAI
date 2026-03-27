import { BenchmarksDashboard } from "@/components/benchmarks-dashboard";
import { PageIntro } from "@/components/page-intro";
import { fetchFromApi } from "@/lib/api-client";
import type { BenchmarkRunListItem } from "@/lib/types";

export default async function BenchmarksPage() {
  const benchmarks = await fetchFromApi<BenchmarkRunListItem[]>("/api/benchmarks?limit=200");
  const running = benchmarks.filter((item) => item.status === "running" || item.status === "queued").length;
  const latest = benchmarks[0] ?? null;

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Benchmarks"
        title="System performance and throughput benchmarks"
        description="Launch configurable benchmark runs, compare saved results, and inspect per-cycle queue behavior in one place."
        meta={[
          `${benchmarks.length} saved runs`,
          `${running} active`,
          latest ? `latest: ${latest.name}` : "no benchmarks yet"
        ]}
      />
      <BenchmarksDashboard initialBenchmarks={benchmarks} />
    </div>
  );
}