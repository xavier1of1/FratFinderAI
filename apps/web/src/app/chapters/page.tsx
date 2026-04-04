import { ChaptersDashboard } from "@/components/chapters-dashboard";
import { PageIntro } from "@/components/page-intro";
import { fetchFromApi } from "@/lib/api-client";
import type { ChapterListResponse, ChapterMapStateSummary } from "@/lib/types";

export default async function ChaptersPage() {
  const [data, mapSummary] = await Promise.all([
    fetchFromApi<ChapterListResponse>("/api/chapters?limit=5000&includeMeta=true"),
    fetchFromApi<ChapterMapStateSummary[]>("/api/chapters/map-summary")
  ]);
  const loadedChapters = data.items;
  const withWebsite = loadedChapters.filter((chapter) => Boolean(chapter.websiteUrl)).length;
  const withInstagram = loadedChapters.filter((chapter) => Boolean(chapter.instagramUrl)).length;
  const withEmail = loadedChapters.filter((chapter) => Boolean(chapter.contactEmail)).length;

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Chapters"
        title="Coverage view for loaded fraternity chapters"
        description="This page is for browsing chapter records, spotting coverage gaps, and showing where websites, Instagram profiles, and emails have already been found."
        meta={[
          `${loadedChapters.length} loaded / ${data.totalCount} total chapters`,
          `${withWebsite} websites`,
          `${withInstagram} Instagrams`,
          `${withEmail} emails`
        ]}
      />
      <ChaptersDashboard
        chapters={loadedChapters}
        mapSummary={mapSummary}
        totalChapterCount={data.totalCount}
        fraternityOptions={data.fraternitySlugs}
        stateOptions={data.stateOptions}
        statusOptions={data.chapterStatuses}
      />
    </div>
  );
}