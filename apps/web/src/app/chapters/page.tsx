import { ChaptersDashboard } from "@/components/chapters-dashboard";
import { PageIntro } from "@/components/page-intro";
import { fetchFromApi } from "@/lib/api-client";
import type { ChapterListResponse, ChapterMapStateSummary } from "@/lib/types";

const DEFAULT_PAGE_SIZE = 250;
const ALLOWED_PAGE_SIZES = new Set([100, 250, 500, 1000]);

function parsePositiveInt(value: string | string[] | undefined, fallback: number) {
  const raw = Array.isArray(value) ? value[0] : value;
  const parsed = Number(raw);
  if (Number.isNaN(parsed) || parsed < 1) {
    return fallback;
  }
  return Math.floor(parsed);
}

export default async function ChaptersPage({
  searchParams
}: {
  searchParams?: Record<string, string | string[] | undefined>;
}) {
  const requestedPage = parsePositiveInt(searchParams?.page, 1);
  const requestedPageSize = parsePositiveInt(searchParams?.pageSize, DEFAULT_PAGE_SIZE);
  const pageSize = ALLOWED_PAGE_SIZES.has(requestedPageSize) ? requestedPageSize : DEFAULT_PAGE_SIZE;
  const currentPage = Math.max(1, requestedPage);
  const offset = (currentPage - 1) * pageSize;

  const [data, mapSummary] = await Promise.all([
    fetchFromApi<ChapterListResponse>(`/api/chapters?limit=${pageSize}&offset=${offset}&includeMeta=true`),
    fetchFromApi<ChapterMapStateSummary[]>("/api/chapters/map-summary")
  ]);
  const loadedChapters = data.items;
  const withWebsite = loadedChapters.filter((chapter) => Boolean(chapter.websiteUrl)).length;
  const withInstagram = loadedChapters.filter((chapter) => Boolean(chapter.instagramUrl)).length;
  const withEmail = loadedChapters.filter((chapter) => Boolean(chapter.contactEmail)).length;
  const totalPages = Math.max(1, Math.ceil(data.totalCount / pageSize));
  const visibleStart = data.totalCount === 0 ? 0 : offset + 1;
  const visibleEnd = offset + loadedChapters.length;

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Chapters"
        title="Coverage view for loaded fraternity chapters"
        description="This page is for browsing chapter records, spotting coverage gaps, and showing where websites, Instagram profiles, and emails have already been found."
        meta={[
          `${visibleStart}-${visibleEnd} of ${data.totalCount} chapters`,
          `Page ${Math.min(currentPage, totalPages)} of ${totalPages}`,
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
        currentPage={Math.min(currentPage, totalPages)}
        pageSize={pageSize}
        totalPages={totalPages}
      />
    </div>
  );
}
