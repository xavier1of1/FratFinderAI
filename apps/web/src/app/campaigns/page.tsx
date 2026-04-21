import { CampaignsDashboard } from "@/components/campaigns-dashboard";
import { PageIntro } from "@/components/page-intro";
import { getCampaignRunCounts, listCampaignRuns } from "@/lib/repositories/campaign-run-repository";
import { listCrawlRuns } from "@/lib/repositories/crawl-run-repository";
import type { CampaignRun, CrawlRunListItem } from "@/lib/types";

export const dynamic = "force-dynamic";

export default async function CampaignsPage() {
  const [campaigns, runs, counts] = await Promise.all([
    listCampaignRuns(100) as Promise<CampaignRun[]>,
    listCrawlRuns(800) as Promise<CrawlRunListItem[]>,
    getCampaignRunCounts()
  ]);
  const running = counts.running + counts.queued;
  const latest = campaigns[0] ?? null;

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Campaigns"
        title="Long-run multi-fraternity benchmark campaigns"
        description="Launch 20-fraternity style validation campaigns, watch queue health live, and compare coverage, throughput, and tuning decisions in one place."
        meta={[
          `${counts.total} saved campaigns`,
          `${running} active`,
          latest ? `latest: ${latest.name}` : "no campaigns yet"
        ]}
      />
      <CampaignsDashboard initialCampaigns={campaigns} initialRuns={runs} summaryCounts={counts} />
    </div>
  );
}


