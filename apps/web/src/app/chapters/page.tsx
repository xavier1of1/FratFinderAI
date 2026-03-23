import { TagPill } from "@/components/tag-pill";
import { fetchFromApi } from "@/lib/api-client";
import type { ChapterListItem } from "@/lib/types";

function fieldTone(state: string | undefined): "neutral" | "info" | "warning" {
  if (state === "found") return "info";
  if (state === "low_confidence" || state === "missing") return "warning";
  return "neutral";
}

function renderFieldState(state: string | undefined) {
  return <TagPill label={state ?? "unknown"} tone={fieldTone(state)} />;
}

export default async function ChaptersPage() {
  const data = await fetchFromApi<ChapterListItem[]>("/api/chapters?limit=200");

  return (
    <section className="panel">
      <h2>Chapters</h2>
      <div className="tableWrap">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Fraternity</th>
              <th>University</th>
              <th>Location</th>
              <th>Status</th>
              <th>Website</th>
              <th>Instagram</th>
              <th>Email</th>
            </tr>
          </thead>
          <tbody>
            {data.map((chapter) => (
              <tr key={chapter.id}>
                <td>{chapter.name}</td>
                <td>{chapter.fraternitySlug}</td>
                <td>{chapter.universityName ?? <span className="muted">Unknown</span>}</td>
                <td>
                  {chapter.city ?? "?"}, {chapter.state ?? "?"}
                </td>
                <td>{chapter.chapterStatus}</td>
                <td>
                  {chapter.websiteUrl ? (
                    <a href={chapter.websiteUrl} target="_blank" rel="noreferrer">
                      Link
                    </a>
                  ) : (
                    <span className="muted">Missing</span>
                  )}
                  <div className="submeta">{renderFieldState(chapter.fieldStates.website_url)}</div>
                </td>
                <td>
                  {chapter.instagramUrl ?? <span className="muted">Missing</span>}
                  <div className="submeta">{renderFieldState(chapter.fieldStates.instagram_url)}</div>
                </td>
                <td>
                  {chapter.contactEmail ?? <span className="muted">Missing</span>}
                  <div className="submeta">{renderFieldState(chapter.fieldStates.contact_email)}</div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
