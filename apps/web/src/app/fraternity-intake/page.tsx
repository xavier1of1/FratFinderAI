import { FraternityIntakeDashboard } from "@/components/fraternity-intake-dashboard";
import { PageIntro } from "@/components/page-intro";
import { fetchFromApi } from "@/lib/api-client";
import type { FraternityCrawlRequest } from "@/lib/types";

export default async function FraternityIntakePage() {
  const requests = await fetchFromApi<FraternityCrawlRequest[]>("/api/fraternity-crawl-requests?limit=200");
  const activeCount = requests.filter((item) => item.status === "queued" || item.status === "running").length;

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Fraternity Intake"
        title="Suggest and schedule staged crawl requests"
        description="Submit a fraternity name, track V3 request-worker execution, and monitor crawl plus enrichment progress across website, email, and Instagram jobs."
        meta={[`${requests.length} saved requests`, `${activeCount} active`, requests[0] ? `latest: ${requests[0].fraternityName}` : "no requests yet"]}
      />
      <FraternityIntakeDashboard initialRequests={requests} />
    </div>
  );
}
