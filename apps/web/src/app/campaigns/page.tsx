import { CampaignsDashboard } from "@/components/campaigns-dashboard";
import { PageIntro } from "@/components/page-intro";
import { fetchFromApi } from "@/lib/api-client";
import type { CampaignRun, CrawlRunListItem } from "@/lib/types";

export default async function CampaignsPage() {
  const [campaigns, runs] = await Promise.all([
    fetchFromApi<CampaignRun[]>("/api/campaign-runs?limit=100"),
    fetchFromApi<CrawlRunListItem[]>("/api/runs?limit=800")
  ]);
  const running = campaigns.filter((item) => item.status === "running" || item.status === "queued").length;
  const latest = campaigns[0] ?? null;

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Campaigns"
        title="Long-run multi-fraternity benchmark campaigns"
        description="Launch 20-fraternity style validation campaigns, watch queue health live, and compare coverage, throughput, and tuning decisions in one place."
        meta={[
          `${campaigns.length} saved campaigns`,
          `${running} active`,
          latest ? `latest: ${latest.name}` : "no campaigns yet"
        ]}
      />
      <CampaignsDashboard initialCampaigns={campaigns} initialRuns={runs} />
    </div>
  );
}


