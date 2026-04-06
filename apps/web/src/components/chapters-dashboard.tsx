"use client";

import { useEffect, useMemo, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

import { TagPill } from "@/components/tag-pill";
import { instagramHandleFromUrl } from "@/lib/social";
import type { ChapterActionResult, ChapterFieldName, ChapterListItem, ChapterMapStateSummary } from "@/lib/types";

type ContactFilter = "all" | "present" | "missing" | "found" | "low_confidence";

interface ApiSuccess<T> {
  success: true;
  data: T;
}

interface ApiFailure {
  success: false;
  error: {
    code: string;
    message: string;
    requestId: string;
  };
}

type ApiEnvelope<T> = ApiSuccess<T> | ApiFailure;

type ChapterDraft = {
  name: string;
  universityName: string;
  city: string;
  state: string;
  chapterStatus: string;
  websiteUrl: string;
  instagramUrl: string;
  contactEmail: string;
};

const STATE_GRID: Record<string, { row: number; column: number }> = {
  AK: { row: 7, column: 1 }, AL: { row: 5, column: 8 }, AR: { row: 4, column: 6 }, AZ: { row: 4, column: 2 },
  CA: { row: 3, column: 1 }, CO: { row: 3, column: 4 }, CT: { row: 2, column: 11 }, DC: { row: 4, column: 11 },
  DE: { row: 3, column: 11 }, FL: { row: 6, column: 10 }, GA: { row: 5, column: 9 }, HI: { row: 7, column: 2 },
  IA: { row: 2, column: 6 }, ID: { row: 2, column: 2 }, IL: { row: 2, column: 7 }, IN: { row: 2, column: 8 },
  KS: { row: 3, column: 5 }, KY: { row: 3, column: 8 }, LA: { row: 5, column: 6 }, MA: { row: 1, column: 11 },
  MD: { row: 3, column: 10 }, ME: { row: 1, column: 12 }, MI: { row: 1, column: 8 }, MN: { row: 1, column: 6 },
  MO: { row: 3, column: 6 }, MS: { row: 5, column: 7 }, MT: { row: 1, column: 3 }, NC: { row: 4, column: 10 },
  ND: { row: 1, column: 5 }, NE: { row: 2, column: 5 }, NH: { row: 1, column: 10 }, NJ: { row: 2, column: 10 },
  NM: { row: 4, column: 3 }, NV: { row: 2, column: 1 }, NY: { row: 1, column: 9 }, OH: { row: 2, column: 9 },
  OK: { row: 4, column: 5 }, OR: { row: 2, column: 1 }, PA: { row: 2, column: 10 }, RI: { row: 1, column: 12 },
  SC: { row: 5, column: 10 }, SD: { row: 1, column: 4 }, TN: { row: 4, column: 8 }, TX: { row: 5, column: 5 },
  UT: { row: 3, column: 2 }, VA: { row: 4, column: 9 }, VT: { row: 1, column: 10 }, WA: { row: 1, column: 1 },
  WI: { row: 1, column: 7 }, WV: { row: 3, column: 9 }, WY: { row: 2, column: 3 }
};

const STATE_CODES = Object.keys(STATE_GRID).sort();
const STATE_NAME_TO_CODE: Record<string, string> = {
  ALABAMA: "AL", ALASKA: "AK", ARIZONA: "AZ", ARKANSAS: "AR", CALIFORNIA: "CA", COLORADO: "CO", CONNECTICUT: "CT", DELAWARE: "DE", "DISTRICT OF COLUMBIA": "DC",
  FLORIDA: "FL", GEORGIA: "GA", HAWAII: "HI", IDAHO: "ID", ILLINOIS: "IL", INDIANA: "IN", IOWA: "IA", KANSAS: "KS", KENTUCKY: "KY", LOUISIANA: "LA",
  MAINE: "ME", MARYLAND: "MD", MASSACHUSETTS: "MA", MICHIGAN: "MI", MINNESOTA: "MN", MISSISSIPPI: "MS", MISSOURI: "MO", MONTANA: "MT", NEBRASKA: "NE", NEVADA: "NV",
  "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY", "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", OHIO: "OH", OKLAHOMA: "OK", OREGON: "OR", PENNSYLVANIA: "PA",
  "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD", TENNESSEE: "TN", TEXAS: "TX", UTAH: "UT", VERMONT: "VT", VIRGINIA: "VA", WASHINGTON: "WA", "WEST VIRGINIA": "WV",
  WISCONSIN: "WI", WYOMING: "WY"
};

function normalizeStateCode(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const normalized = value.trim().toUpperCase();
  if (STATE_CODES.includes(normalized)) {
    return normalized;
  }
  return STATE_NAME_TO_CODE[normalized] ?? null;
}
const DEFAULT_RERUN_FIELDS: ChapterFieldName[] = ["find_website", "find_email", "find_instagram"];
const MAX_VISIBLE_STATE_DOTS = 24;
const PAGE_SIZE_OPTIONS = [100, 250, 500, 1000];

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

function buildDraft(chapter: ChapterListItem): ChapterDraft {
  return {
    name: chapter.name,
    universityName: chapter.universityName ?? "",
    city: chapter.city ?? "",
    state: chapter.state ?? "",
    chapterStatus: chapter.chapterStatus,
    websiteUrl: chapter.websiteUrl ?? "",
    instagramUrl: chapter.instagramUrl ?? "",
    contactEmail: chapter.contactEmail ?? ""
  };
}

async function unwrapResponse<T>(response: Response): Promise<T> {
  const payload = (await response.json()) as ApiEnvelope<T>;
  if (!response.ok || !payload.success) {
    if (!payload.success) {
      throw new Error(`${payload.error.code}: ${payload.error.message}`);
    }
    throw new Error(`Request failed with ${response.status}`);
  }
  return payload.data;
}

export function ChaptersDashboard({
  chapters: initialChapters,
  mapSummary,
  totalChapterCount: initialTotalChapterCount,
  fraternityOptions: initialFraternityOptions,
  stateOptions: initialStateOptions,
  statusOptions: initialStatusOptions,
  currentPage,
  pageSize,
  totalPages
}: {
  chapters: ChapterListItem[];
  mapSummary: ChapterMapStateSummary[];
  totalChapterCount: number;
  fraternityOptions: string[];
  stateOptions: string[];
  statusOptions: string[];
  currentPage: number;
  pageSize: number;
  totalPages: number;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [chapters, setChapters] = useState<ChapterListItem[]>(initialChapters);
  const [totalChapterCount, setTotalChapterCount] = useState<number>(Math.max(initialTotalChapterCount, initialChapters.length));
  const [nameFilter, setNameFilter] = useState("");
  const [fraternityFilter, setFraternityFilter] = useState("all");
  const [universityFilter, setUniversityFilter] = useState("");
  const [stateFilter, setStateFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");
  const [websiteFilter, setWebsiteFilter] = useState<ContactFilter>("all");
  const [instagramFilter, setInstagramFilter] = useState<ContactFilter>("all");
  const [emailFilter, setEmailFilter] = useState<ContactFilter>("all");
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [rerunFields, setRerunFields] = useState<ChapterFieldName[]>(DEFAULT_RERUN_FIELDS);
  const [draft, setDraft] = useState<ChapterDraft | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const pageNumbers = useMemo(() => {
    if (totalPages <= 1) {
      return [1];
    }
    const pages = new Set<number>([1, totalPages]);
    for (let page = Math.max(1, currentPage - 2); page <= Math.min(totalPages, currentPage + 2); page += 1) {
      pages.add(page);
    }
    return Array.from(pages).sort((a, b) => a - b);
  }, [currentPage, totalPages]);

  const pageRangeSummary = useMemo(() => {
    if (totalChapterCount === 0) {
      return "No chapters available.";
    }
    const start = (currentPage - 1) * pageSize + 1;
    const end = Math.min(totalChapterCount, start + chapters.length - 1);
    return `Showing chapters ${start}-${end} of ${totalChapterCount}.`;
  }, [chapters.length, currentPage, pageSize, totalChapterCount]);

  const fraternityOptions = useMemo(
    () => Array.from(new Set([...initialFraternityOptions, ...chapters.map((chapter) => chapter.fraternitySlug)])).sort(),
    [chapters, initialFraternityOptions]
  );
  const statusOptions = useMemo(
    () => Array.from(new Set([...initialStatusOptions, ...chapters.map((chapter) => chapter.chapterStatus)])).sort(),
    [chapters, initialStatusOptions]
  );
  const stateOptions = useMemo(
    () =>
      Array.from(
        new Set([
          ...initialStateOptions,
          ...(chapters.map((chapter) => chapter.state).filter(Boolean) as string[])
        ])
      ).sort(),
    [chapters, initialStateOptions]
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
  }, [chapters, emailFilter, fraternityFilter, instagramFilter, nameFilter, stateFilter, statusFilter, universityFilter, websiteFilter]);

  const selectedChapters = useMemo(() => chapters.filter((chapter) => selectedIds.includes(chapter.id)), [chapters, selectedIds]);
  const activeChapter = selectedChapters.length === 1 ? selectedChapters[0]! : null;

  useEffect(() => {
    if (activeChapter) {
      setDraft(buildDraft(activeChapter));
    } else {
      setDraft(null);
    }
  }, [activeChapter?.id]);

  const mapCounts = useMemo(() => {
    const summaryCounts = new Map(mapSummary.map((item) => [item.stateCode, item.chapterCount]));
    const summaryTotal = Array.from(summaryCounts.values()).reduce((sum, count) => sum + Number(count || 0), 0);
    if (summaryTotal > 0) {
      return summaryCounts;
    }
    const fallback = new Map<string, number>();
    for (const chapter of chapters) {
      const code = normalizeStateCode(chapter.state);
      if (!code) {
        continue;
      }
      fallback.set(code, (fallback.get(code) ?? 0) + 1);
    }
    return fallback;
  }, [chapters, mapSummary]);

  const allFilteredSelected = filteredChapters.length > 0 && filteredChapters.every((chapter) => selectedIds.includes(chapter.id));

  function toggleSelect(id: string) {
    setSelectedIds((current) => (current.includes(id) ? current.filter((item) => item !== id) : [...current, id]));
  }

  function toggleSelectAllFiltered() {
    setSelectedIds((current) => {
      if (allFilteredSelected) {
        return current.filter((id) => !filteredChapters.some((chapter) => chapter.id === id));
      }
      return Array.from(new Set([...current, ...filteredChapters.map((chapter) => chapter.id)]));
    });
  }

  function toggleRerunField(field: ChapterFieldName) {
    setRerunFields((current) =>
      current.includes(field) ? current.filter((item) => item !== field) : [...current, field]
    );
  }

  function updatePagination(nextPage: number, nextPageSize = pageSize) {
    const params = new URLSearchParams(searchParams.toString());
    params.set("page", String(Math.max(1, Math.min(nextPage, totalPages))));
    params.set("pageSize", String(nextPageSize));
    router.push(`${pathname}?${params.toString()}`);
  }

  function changePageSize(nextPageSize: number) {
    const params = new URLSearchParams(searchParams.toString());
    params.set("pageSize", String(nextPageSize));
    params.set("page", "1");
    router.push(`${pathname}?${params.toString()}`);
  }

  async function requestRerun() {
    if (!selectedIds.length || !rerunFields.length) {
      return;
    }
    setIsSaving(true);
    setErrorMessage(null);
    setActionMessage(null);
    try {
      const result = await unwrapResponse<ChapterActionResult>(
        await fetch("/api/chapters/actions", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action: "rerun", chapterIds: selectedIds, fieldNames: rerunFields })
        })
      );
      setActionMessage(`Queued ${result.affectedCount} rerun jobs across ${selectedIds.length} selected chapters.${result.missingSourceCount ? ` ${result.missingSourceCount} chapters had no source provenance.` : ""}`);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    } finally {
      setIsSaving(false);
    }
  }

  async function deleteSelected() {
    if (!selectedIds.length) {
      return;
    }
    if (!window.confirm(`Delete ${selectedIds.length} chapter record(s)? This also removes queued field jobs and provenance for those chapters.`)) {
      return;
    }
    setIsSaving(true);
    setErrorMessage(null);
    setActionMessage(null);
    try {
      const result = await unwrapResponse<ChapterActionResult>(
        await fetch("/api/chapters/actions", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action: "delete", chapterIds: selectedIds })
        })
      );
      setChapters((current) => current.filter((chapter) => !selectedIds.includes(chapter.id)));
      setTotalChapterCount((current) => Math.max(0, current - result.affectedCount));
      setSelectedIds([]);
      setActionMessage(`Deleted ${result.affectedCount} chapter record(s).`);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    } finally {
      setIsSaving(false);
    }
  }

  async function saveChapter() {
    if (!activeChapter || !draft) {
      return;
    }
    setIsSaving(true);
    setErrorMessage(null);
    setActionMessage(null);
    try {
      const updated = await unwrapResponse<ChapterListItem>(
        await fetch(`/api/chapters/${activeChapter.id}`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(draft)
        })
      );
      setChapters((current) => current.map((chapter) => (chapter.id === updated.id ? updated : chapter)));
      setActionMessage(`Saved edits for ${updated.name}.`);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    } finally {
      setIsSaving(false);
    }
  }

  return (
    <div className="chaptersStack">
      <section className="panel">
        <div className="chaptersHeaderRow">
          <div>
            <h2>Chapters</h2>
            <p className="muted mapNote">{pageRangeSummary}</p>
            <p className="muted mapNote">Filtered to {filteredChapters.length} of {chapters.length} chapters loaded on this page.</p>
            <p className="muted mapNote">Map counts use normalized state data across the full chapter dataset.</p>
          </div>
          <div className="mapLegend">
            <span className="markerDot" />
            <span>Dots show relative chapter volume and the number shows the exact state total.</span>
          </div>
        </div>
        <div className="stateGridMap" aria-label="United States chapter map">
          {STATE_CODES.map((stateCode) => {
            const chapterCount = mapCounts.get(stateCode) ?? 0;
            const visibleDots = Math.min(chapterCount, MAX_VISIBLE_STATE_DOTS);
            const position = STATE_GRID[stateCode]!;
            return (
              <article
                key={stateCode}
                className={`stateTile${chapterCount ? " hasChapters" : ""}`}
                style={{ gridColumn: position.column, gridRow: position.row }}
              >
                <div className="stateTileHeader">
                  <strong>{stateCode}</strong>
                  <span>{chapterCount}</span>
                </div>
                <div className="stateMarkers">
                  {Array.from({ length: visibleDots }, (_, index) => (
                    <span
                      key={`${stateCode}-${index}`}
                      className="markerDot"
                      title={`${chapterCount} mapped chapter${chapterCount === 1 ? "" : "s"} in ${stateCode}`}
                    />
                  ))}
                </div>
              </article>
            );
          })}
        </div>
      </section>

      <section className="panel">
        <div className="chaptersActionHeader">
          <div>
            <h2>Operator Actions</h2>
            <p className="sectionDescription">Select chapters to request reruns, remove bad records, or edit a single chapter directly from the dashboard.</p>
          </div>
          <div className="pageIntroMeta">
            <span className="pageIntroMetaItem">{selectedIds.length} selected</span>
            <span className="pageIntroMetaItem">{activeChapter ? "single-edit mode" : "bulk mode"}</span>
          </div>
        </div>

        <div className="chaptersControlGrid">
          <div className="heroAsideCard">
            <p className="eyebrow">Bulk Actions</p>
            <div className="heroChecklistItem">
              <strong>Request reruns</strong>
              <span>Queue fresh website, email, or Instagram enrichment jobs for the selected chapters.</span>
            </div>
            <div className="fieldToggleRow">
              {DEFAULT_RERUN_FIELDS.map((field) => (
                <button
                  key={field}
                  type="button"
                  className={`buttonSecondary toggleButton${rerunFields.includes(field) ? " isActiveFilter" : ""}`}
                  onClick={() => toggleRerunField(field)}
                >
                  {field}
                </button>
              ))}
            </div>
            <div className="buttonRow">
              <button type="button" className="buttonPrimaryAuto" disabled={isSaving || selectedIds.length === 0 || rerunFields.length === 0} onClick={() => void requestRerun()}>
                {isSaving ? "Working..." : "Request Rerun"}
              </button>
              <button type="button" className="buttonSecondary" disabled={isSaving || selectedIds.length === 0} onClick={() => void deleteSelected()}>
                Delete Selected
              </button>
            </div>
          </div>

          <div className="heroAsideCard">
            <p className="eyebrow">Selection Summary</p>
            {activeChapter && draft ? (
              <div className="fieldStack">
                <label htmlFor="chapter-name-edit">Chapter Name</label>
                <input id="chapter-name-edit" value={draft.name} onChange={(event) => setDraft((current) => (current ? { ...current, name: event.target.value } : current))} />
                <label htmlFor="chapter-university-edit">University</label>
                <input id="chapter-university-edit" value={draft.universityName} onChange={(event) => setDraft((current) => (current ? { ...current, universityName: event.target.value } : current))} />
                <div className="benchmarkFormGrid">
                  <div className="fieldStack">
                    <label htmlFor="chapter-city-edit">City</label>
                    <input id="chapter-city-edit" value={draft.city} onChange={(event) => setDraft((current) => (current ? { ...current, city: event.target.value } : current))} />
                  </div>
                  <div className="fieldStack">
                    <label htmlFor="chapter-state-edit">State</label>
                    <input id="chapter-state-edit" value={draft.state} onChange={(event) => setDraft((current) => (current ? { ...current, state: event.target.value } : current))} />
                  </div>
                </div>
                <label htmlFor="chapter-status-edit">Chapter Status</label>
                <input id="chapter-status-edit" value={draft.chapterStatus} onChange={(event) => setDraft((current) => (current ? { ...current, chapterStatus: event.target.value } : current))} />
                <label htmlFor="chapter-website-edit">Website</label>
                <input id="chapter-website-edit" value={draft.websiteUrl} onChange={(event) => setDraft((current) => (current ? { ...current, websiteUrl: event.target.value } : current))} placeholder="https://..." />
                <label htmlFor="chapter-instagram-edit">Instagram</label>
                <input id="chapter-instagram-edit" value={draft.instagramUrl} onChange={(event) => setDraft((current) => (current ? { ...current, instagramUrl: event.target.value } : current))} placeholder="https://instagram.com/..." />
                <label htmlFor="chapter-email-edit">Email</label>
                <input id="chapter-email-edit" value={draft.contactEmail} onChange={(event) => setDraft((current) => (current ? { ...current, contactEmail: event.target.value } : current))} placeholder="chapter@example.edu" />
                <div className="buttonRow">
                  <button type="button" className="buttonPrimaryAuto" disabled={isSaving} onClick={() => void saveChapter()}>
                    {isSaving ? "Saving..." : "Save Chapter"}
                  </button>
                </div>
              </div>
            ) : (
              <div className="heroChecklistItem">
                <strong>{selectedIds.length ? `${selectedIds.length} chapters selected` : "No chapters selected"}</strong>
                <span>{selectedIds.length === 1 ? "Pick one chapter to edit its text fields here." : "Select a single chapter to enable text editing, or keep multiple selected for bulk actions."}</span>
              </div>
            )}
          </div>
        </div>

        {actionMessage ? <p className="benchmarkSuccess">{actionMessage}</p> : null}
        {errorMessage ? <p className="benchmarkError">{errorMessage}</p> : null}
      </section>

      <section className="panel">
        <div className="tableWrap">
          <table>
            <thead>
              <tr>
                <th>
                  <input type="checkbox" checked={allFilteredSelected} onChange={toggleSelectAllFiltered} aria-label="Select all filtered chapters" />
                </th>
                <th>Name</th>
                <th>Fraternity</th>
                <th>Source</th>
                <th>University</th>
                <th>Location</th>
                <th>Status</th>
                <th>Website</th>
                <th>Instagram</th>
                <th>Email</th>
              </tr>
              <tr className="filterRow">
                <th />
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
                <th />
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
                <tr key={chapter.id} className={selectedIds.includes(chapter.id) ? "rowSelected" : ""}>
                  <td>
                    <input type="checkbox" checked={selectedIds.includes(chapter.id)} onChange={() => toggleSelect(chapter.id)} aria-label={`Select ${chapter.name}`} />
                  </td>
                  <td>{chapter.name}</td>
                  <td>{chapter.fraternitySlug}</td>
                  <td>{chapter.sourceSlug ?? <span className="muted">n/a</span>}</td>
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
                      <a href={chapter.instagramUrl} target="_blank" rel="noreferrer">
                        @{instagramHandleFromUrl(chapter.instagramUrl) ?? "profile"}
                      </a>
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
        <div className="chaptersHeaderRow" style={{ marginTop: 20, alignItems: "center", gap: 16, flexWrap: "wrap" }}>
          <div className="fieldStack" style={{ minWidth: 180 }}>
            <label htmlFor="chapters-page-size">Page Size</label>
            <select
              id="chapters-page-size"
              className="filterSelect"
              value={pageSize}
              onChange={(event) => changePageSize(Number(event.target.value))}
            >
              {PAGE_SIZE_OPTIONS.map((option) => (
                <option key={option} value={option}>
                  {option} chapters
                </option>
              ))}
            </select>
          </div>
          <div className="buttonRow" style={{ flexWrap: "wrap" }}>
            <button
              type="button"
              className="buttonSecondary"
              disabled={currentPage <= 1}
              onClick={() => updatePagination(currentPage - 1)}
            >
              Previous Page
            </button>
            {pageNumbers.map((page, index) => {
              const previousPage = pageNumbers[index - 1];
              const showGap = previousPage !== undefined && page - previousPage > 1;
              return (
                <div key={page} style={{ display: "contents" }}>
                  {showGap ? <span className="muted" style={{ alignSelf: "center" }}>...</span> : null}
                  <button
                    type="button"
                    className={`buttonSecondary toggleButton${page === currentPage ? " isActiveFilter" : ""}`}
                    onClick={() => updatePagination(page)}
                  >
                    {page}
                  </button>
                </div>
              );
            })}
            <button
              type="button"
              className="buttonSecondary"
              disabled={currentPage >= totalPages}
              onClick={() => updatePagination(currentPage + 1)}
            >
              Next Page
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}
