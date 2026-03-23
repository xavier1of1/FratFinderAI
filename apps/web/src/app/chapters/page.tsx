import { fetchFromApi } from "@/lib/api-client";
import { ChaptersDashboard } from "@/components/chapters-dashboard";
import type { ChapterListItem } from "@/lib/types";

export default async function ChaptersPage() {
  const data = await fetchFromApi<ChapterListItem[]>("/api/chapters?limit=500");

  return <ChaptersDashboard chapters={data} />;
}
