"use client";

import { useMemo, useState } from "react";

import { TagPill } from "@/components/tag-pill";
import type { ChapterListItem } from "@/lib/types";

type ContactFilter = "all" | "present" | "missing" | "found" | "low_confidence";

const STATE_GRID: Record<string, { row: number; column: number }> = {
  AK: { row: 7, column: 1 },
  AL: { row: 5, column: 8 },
  AR: { row: 4, column: 6 },
  AZ: { row: 4, column: 2 },
  CA: { row: 3, column: 1 },
  CO: { row: 3, column: 4 },
  CT: { row: 2, column: 11 },
  DC: { row: 4, column: 11 },
  DE: { row: 3, column: 11 },
  FL: { row: 6, column: 10 },
  GA: { row: 5, column: 9 },
  HI: { row: 7, column: 2 },
  IA: { row: 2, column: 6 },
  ID: { row: 2, column: 2 },
  IL: { row: 2, column: 7 },
  IN: { row: 2, column: 8 },
  KS: { row: 3, column: 5 },
  KY: { row: 3, column: 8 },
  LA: { row: 5, column: 6 },
  MA: { row: 1, column: 11 },
  MD: { row: 3, column: 10 },
  ME: { row: 1, column: 12 },
  MI: { row: 1, column: 8 },
  MN: { row: 1, column: 6 },
  MO: { row: 3, column: 6 },
  MS: { row: 5, column: 7 },
  MT: { row: 1, column: 3 },
  NC: { row: 4, column: 10 },
  ND: { row: 1, column: 5 },
  NE: { row: 2, column: 5 },
  NH: { row: 1, column: 10 },
  NJ: { row: 2, column: 10 },
  NM: { row: 4, column: 3 },
  NV: { row: 2, column: 1 },
  NY: { row: 1, column: 9 },
  OH: { row: 2, column: 9 },
  OK: { row: 4, column: 5 },
  OR: { row: 2, column: 1 },
  PA: { row: 2, column: 10 },
  RI: { row: 1, column: 12 },
  SC: { row: 5, column: 10 },
  SD: { row: 1, column: 4 },
  TN: { row: 4, column: 8 },
  TX: { row: 5, column: 5 },
  UT: { row: 3, column: 2 },
  VA: { row: 4, column: 9 },
  VT: { row: 1, column: 10 },
  WA: { row: 1, column: 1 },
  WI: { row: 1, column: 7 },
  WV: { row: 3, column: 9 },
  WY: { row: 2, column: 3 },
};

const STATE_CODES = Object.keys(STATE_GRID).sort();

function fieldTone(state: string | undefined): "neutral" | "info" | "warning" {
  if (state === "found") return "info";
  if (state === "low_confidence" || state === "missing") return "warning";
  return "neutral";
}

function renderFieldState(state: string | undefined) {
  return <TagPill label={state ?? "unknown"} tone={fieldTone(state)} />;
}

function matchesContactFilter(value: string | null, state: string | undefined, filter: ContactFilter) {
  switch (filter) {
    case "present":
      return Boolean(value);
    case "missing":
      return !value;
    case "found":
      return state === "found";
    case "low_confidence":
      return state === "low_confidence";
    default:
      return true;
  }
}

export function ChaptersDashboard({ chapters }: { chapters: ChapterListItem[] }) {
  const [nameFilter, setNameFilter] = useState("");
  const [fraternityFilter, setFraternityFilter] = useState("all");
  const [universityFilter, setUniversityFilter] = useState("");
  const [stateFilter, setStateFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");
  const [websiteFilter, setWebsiteFilter] = useState<ContactFilter>("all");
  const [instagramFilter, setInstagramFilter] = useState<ContactFilter>("all");
  const [emailFilter, setEmailFilter] = useState<ContactFilter>("all");

  const fraternityOptions = useMemo(
    () => Array.from(new Set(chapters.map((chapter) => chapter.fraternitySlug))).sort(),
    [chapters]
  );
  const statusOptions = useMemo(
    () => Array.from(new Set(chapters.map((chapter) => chapter.chapterStatus))).sort(),
    [chapters]
  );
  const stateOptions = useMemo(
    () => Array.from(new Set(chapters.map((chapter) => chapter.state).filter(Boolean) as string[])).sort(),
    [chapters]
  );

  const filteredChapters = useMemo(() => {
    const normalizedName = nameFilter.trim().toLowerCase();
    const normalizedUniversity = universityFilter.trim().toLowerCase();

    return chapters.filter((chapter) => {
      if (normalizedName && !chapter.name.toLowerCase().includes(normalizedName)) return false;
      if (fraternityFilter !== "all" && chapter.fraternitySlug !== fraternityFilter) return false;
      if (normalizedUniversity && !(chapter.universityName ?? "").toLowerCase().includes(normalizedUniversity)) return false;
      if (stateFilter !== "all" && chapter.state !== stateFilter) return false;
      if (statusFilter !== "all" && chapter.chapterStatus !== statusFilter) return false;
      if (!matchesContactFilter(chapter.websiteUrl, chapter.fieldStates.website_url, websiteFilter)) return false;
      if (!matchesContactFilter(chapter.instagramUrl, chapter.fieldStates.instagram_url, instagramFilter)) return false;
      if (!matchesContactFilter(chapter.contactEmail, chapter.fieldStates.contact_email, emailFilter)) return false;
      return true;
    });
  }, [
    chapters,
    emailFilter,
    fraternityFilter,
    instagramFilter,
    nameFilter,
    stateFilter,
    statusFilter,
    universityFilter,
    websiteFilter,
  ]);

  const chaptersByState = useMemo(() => {
    const grouped = new Map<string, ChapterListItem[]>();
    for (const chapter of filteredChapters) {
      if (!chapter.state) continue;
      const existing = grouped.get(chapter.state) ?? [];
      existing.push(chapter);
      grouped.set(chapter.state, existing);
    }
    return grouped;
  }, [filteredChapters]);

  return (
    <div className="chaptersStack">
      <section className="panel">
        <div className="chaptersHeaderRow">
          <div>
            <h2>Chapters</h2>
            <p className="muted mapNote">Showing {filteredChapters.length} of {chapters.length} loaded chapters.</p>
          </div>
          <div className="mapLegend">
            <span className="markerDot" />
            <span>Each dot represents one chapter.</span>
          </div>
        </div>
        <div className="stateGridMap" aria-label="United States chapter map">
          {STATE_CODES.map((stateCode) => {
            const stateChapters = chaptersByState.get(stateCode) ?? [];
            const position = STATE_GRID[stateCode]!;
            return (
              <article
                key={stateCode}
                className={`stateTile${stateChapters.length ? " hasChapters" : ""}`}
                style={{ gridColumn: position.column, gridRow: position.row }}
              >
                <div className="stateTileHeader">
                  <strong>{stateCode}</strong>
                  <span>{stateChapters.length}</span>
                </div>
                <div className="stateMarkers">
                  {stateChapters.map((chapter) => (
                    <span key={chapter.id} className="markerDot" title={`${chapter.name} — ${chapter.universityName ?? "Unknown school"}`} />
                  ))}
                </div>
              </article>
            );
          })}
        </div>
      </section>

      <section className="panel">
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
              <tr className="filterRow">
                <th>
                  <input className="filterInput" value={nameFilter} onChange={(event) => setNameFilter(event.target.value)} placeholder="Filter name" />
                </th>
                <th>
                  <select className="filterSelect" value={fraternityFilter} onChange={(event) => setFraternityFilter(event.target.value)}>
                    <option value="all">All</option>
                    {fraternityOptions.map((option) => (
                      <option key={option} value={option}>{option}</option>
                    ))}
                  </select>
                </th>
                <th>
                  <input className="filterInput" value={universityFilter} onChange={(event) => setUniversityFilter(event.target.value)} placeholder="Filter university" />
                </th>
                <th>
                  <select className="filterSelect" value={stateFilter} onChange={(event) => setStateFilter(event.target.value)}>
                    <option value="all">All</option>
                    {stateOptions.map((option) => (
                      <option key={option} value={option}>{option}</option>
                    ))}
                  </select>
                </th>
                <th>
                  <select className="filterSelect" value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
                    <option value="all">All</option>
                    {statusOptions.map((option) => (
                      <option key={option} value={option}>{option}</option>
                    ))}
                  </select>
                </th>
                <th>
                  <select className="filterSelect" value={websiteFilter} onChange={(event) => setWebsiteFilter(event.target.value as ContactFilter)}>
                    <option value="all">All</option>
                    <option value="present">Has link</option>
                    <option value="missing">Missing</option>
                    <option value="found">Found</option>
                    <option value="low_confidence">Low confidence</option>
                  </select>
                </th>
                <th>
                  <select className="filterSelect" value={instagramFilter} onChange={(event) => setInstagramFilter(event.target.value as ContactFilter)}>
                    <option value="all">All</option>
                    <option value="present">Has link</option>
                    <option value="missing">Missing</option>
                    <option value="found">Found</option>
                    <option value="low_confidence">Low confidence</option>
                  </select>
                </th>
                <th>
                  <select className="filterSelect" value={emailFilter} onChange={(event) => setEmailFilter(event.target.value as ContactFilter)}>
                    <option value="all">All</option>
                    <option value="present">Has email</option>
                    <option value="missing">Missing</option>
                    <option value="found">Found</option>
                    <option value="low_confidence">Low confidence</option>
                  </select>
                </th>
              </tr>
            </thead>
            <tbody>
              {filteredChapters.map((chapter) => (
                <tr key={chapter.id}>
                  <td>{chapter.name}</td>
                  <td>{chapter.fraternitySlug}</td>
                  <td>{chapter.universityName ?? <span className="muted">Unknown</span>}</td>
                  <td>{chapter.city ?? "?"}, {chapter.state ?? "?"}</td>
                  <td>{chapter.chapterStatus}</td>
                  <td>
                    {chapter.websiteUrl ? (
                      <a href={chapter.websiteUrl} target="_blank" rel="noreferrer">Link</a>
                    ) : (
                      <span className="muted">Missing</span>
                    )}
                    <div className="submeta">{renderFieldState(chapter.fieldStates.website_url)}</div>
                  </td>
                  <td>
                    {chapter.instagramUrl ? (
                      <a href={chapter.instagramUrl} target="_blank" rel="noreferrer">Profile</a>
                    ) : (
                      <span className="muted">Missing</span>
                    )}
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
    </div>
  );
}

