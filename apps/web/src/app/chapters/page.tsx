import { redirect } from "next/navigation";

import { ChaptersDashboard } from "@/components/chapters-dashboard";
import { PageIntro } from "@/components/page-intro";
import {
  getChapterListMetadata,
  listChapterMapSummary,
  listChapters
} from "@/lib/repositories/chapter-repository";

const DEFAULT_PAGE_SIZE = 250;
const ALLOWED_PAGE_SIZES = new Set([100, 250, 500, 1000]);
export const dynamic = "force-dynamic";

function parsePositiveInt(value: string | string[] | undefined, fallback: number) {
  const raw = Array.isArray(value) ? value[0] : value;
  const parsed = Number(raw);
  if (Number.isNaN(parsed) || parsed < 1) {
    return fallback;
  }
  return Math.floor(parsed);
}

function normalizeSearchParamValue(value: string | string[] | undefined): string | undefined {
  if (Array.isArray(value)) {
    return value[0];
  }
  return value;
}

function buildCanonicalQueryString(
  searchParams: Record<string, string | string[] | undefined> | undefined,
  page: number,
  pageSize: number
) {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(searchParams ?? {})) {
    if (key === "page" || key === "pageSize") {
      continue;
    }
    const normalized = normalizeSearchParamValue(value);
    if (normalized) {
      params.set(key, normalized);
    }
  }
  params.set("page", String(page));
  params.set("pageSize", String(pageSize));
  return params.toString();
}

export default async function ChaptersPage({
  searchParams
}: {
  searchParams?: Record<string, string | string[] | undefined>;
}) {
  const requestedPage = parsePositiveInt(searchParams?.page, 1);
  const requestedPageSize = parsePositiveInt(searchParams?.pageSize, DEFAULT_PAGE_SIZE);
  const pageSize = ALLOWED_PAGE_SIZES.has(requestedPageSize) ? requestedPageSize : DEFAULT_PAGE_SIZE;
  const metadata = await getChapterListMetadata({});
  const totalPages = Math.max(1, Math.ceil(metadata.totalCount / pageSize));
  const currentPage = metadata.totalCount === 0 ? 1 : Math.min(Math.max(1, requestedPage), totalPages);

  if (requestedPage !== currentPage || requestedPageSize !== pageSize) {
    redirect(`/chapters?${buildCanonicalQueryString(searchParams, currentPage, pageSize)}`);
  }

  const offset = (currentPage - 1) * pageSize;
  const [loadedChapters, mapSummary] = await Promise.all([
    listChapters({ limit: pageSize, offset }),
    listChapterMapSummary()
  ]);
  const withWebsite = loadedChapters.filter((chapter) => Boolean(chapter.websiteUrl)).length;
  const withInstagram = loadedChapters.filter((chapter) => Boolean(chapter.instagramUrl)).length;
  const withEmail = loadedChapters.filter((chapter) => Boolean(chapter.contactEmail)).length;
  const visibleStart = metadata.totalCount === 0 ? 0 : offset + 1;
  const visibleEnd = offset + loadedChapters.length;

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Chapters"
        title="Coverage view for loaded fraternity chapters"
        description="This page is for browsing chapter records, spotting coverage gaps, and showing where websites, Instagram profiles, and emails have already been found."
        meta={[
          `${visibleStart}-${visibleEnd} of ${metadata.totalCount} chapters`,
          `Page ${currentPage} of ${totalPages}`,
          `${withWebsite} websites`,
          `${withInstagram} Instagrams`,
          `${withEmail} emails`
        ]}
      />
      <ChaptersDashboard
        chapters={loadedChapters}
        mapSummary={mapSummary}
        totalChapterCount={metadata.totalCount}
        fraternityOptions={metadata.fraternitySlugs}
        stateOptions={metadata.stateOptions}
        statusOptions={metadata.chapterStatuses}
        currentPage={currentPage}
        pageSize={pageSize}
        totalPages={totalPages}
      />
    </div>
  );
}
