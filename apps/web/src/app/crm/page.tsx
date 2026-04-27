import { CrmDashboard } from "@/components/crm-dashboard";
import { PageIntro } from "@/components/page-intro";
import { getChapterListMetadata } from "@/lib/repositories/chapter-repository";
import { getCrmCampaignCounts, listCrmCampaigns } from "@/lib/repositories/crm-repository";

export const dynamic = "force-dynamic";

export default async function CrmPage() {
  const [campaigns, counts, chapterMetadata] = await Promise.all([
    listCrmCampaigns(100),
    getCrmCampaignCounts(),
    getChapterListMetadata({})
  ]);

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="CRM"
        title="Targeted outreach for chapter contacts"
        description="Build chapter-targeted outreach campaigns, send email through Outlook, and run a tracked Instagram outreach queue without losing chapter-level context."
        meta={[
          `${counts.total} campaigns`,
          `${counts.ready} ready`,
          `${chapterMetadata.withEmailCount} email contacts`,
          `${chapterMetadata.withInstagramCount} Instagram contacts`
        ]}
      />
      <CrmDashboard
        initialCampaigns={campaigns}
        fraternityOptions={chapterMetadata.fraternitySlugs}
        stateOptions={chapterMetadata.stateOptions}
      />
    </div>
  );
}
