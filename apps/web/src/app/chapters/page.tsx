import { ChaptersDashboard } from "@/components/chapters-dashboard";
import { PageIntro } from "@/components/page-intro";
import { fetchFromApi } from "@/lib/api-client";
import type { ChapterListItem } from "@/lib/types";

export default async function ChaptersPage() {
  const data = await fetchFromApi<ChapterListItem[]>("/api/chapters?limit=500");
  const withWebsite = data.filter((chapter) => Boolean(chapter.websiteUrl)).length;
  const withInstagram = data.filter((chapter) => Boolean(chapter.instagramUrl)).length;
  const withEmail = data.filter((chapter) => Boolean(chapter.contactEmail)).length;

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Chapters"
        title="Coverage view for loaded fraternity chapters"
        description="This page is for browsing chapter records, spotting coverage gaps, and showing where websites, Instagram profiles, and emails have already been found."
        meta={[`${data.length} loaded chapters`, `${withWebsite} websites`, `${withInstagram} Instagrams`, `${withEmail} emails`]}
      />
      <ChaptersDashboard chapters={data} />
    </div>
  );
}
