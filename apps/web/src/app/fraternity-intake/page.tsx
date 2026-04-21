import { FraternityIntakeDashboard } from "@/components/fraternity-intake-dashboard";
import { PageIntro } from "@/components/page-intro";
import { getFraternityCrawlRequestCounts, listFraternityCrawlRequests } from "@/lib/repositories/fraternity-crawl-request-repository";
import type { FraternityCrawlRequest } from "@/lib/types";

export const dynamic = "force-dynamic";

export default async function FraternityIntakePage() {
  const [requests, counts] = await Promise.all([
    listFraternityCrawlRequests(200) as Promise<FraternityCrawlRequest[]>,
    getFraternityCrawlRequestCounts()
  ]);
  const activeCount = counts.queued + counts.running;

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Fraternity Intake"
        title="Suggest and schedule staged crawl requests"
        description="Submit a fraternity name, track V3 request-worker execution, and monitor crawl plus enrichment progress across website, email, and Instagram jobs."
        meta={[`${counts.total} saved requests`, `${activeCount} active`, `${counts.draft} awaiting confirmation`, requests[0] ? `latest: ${requests[0].fraternityName}` : "no requests yet"]}
      />
      <FraternityIntakeDashboard initialRequests={requests} summaryCounts={counts} />
    </div>
  );
}
