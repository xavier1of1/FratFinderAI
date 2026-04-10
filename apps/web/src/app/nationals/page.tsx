import { MetricCard } from "@/components/metric-card";
import { PageIntro } from "@/components/page-intro";
import { TagPill } from "@/components/tag-pill";
import { fetchFromApi } from "@/lib/api-client";
import type { NationalProfile } from "@/lib/types";

export default async function NationalsPage() {
  const profiles = await fetchFromApi<NationalProfile[]>("/api/nationals?limit=250");
  const withEmail = profiles.filter((item) => Boolean(item.contactEmail)).length;
  const withInstagram = profiles.filter((item) => Boolean(item.instagramUrl)).length;
  const withPhone = profiles.filter((item) => Boolean(item.phone)).length;

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Nationals"
        title="First-class nationals profiles and contact truth"
        description="Use this page to inspect what the system knows about each national organization separately from chapter rows. Generic nationals contact belongs here unless the page is explicitly chapter-specific."
        meta={[`${profiles.length} organizations`, `${withEmail} with email`, `${withInstagram} with Instagram`, `${withPhone} with phone`]}
      />

      <section className="panel">
        <h2>Nationals Coverage</h2>
        <p className="sectionDescription">This surface is the new source of truth for national-level contact and provenance so chapter rows do not silently inherit generic HQ data.</p>
        <div className="metrics">
          <MetricCard label="Profiles" value={profiles.length} />
          <MetricCard label="With Email" value={withEmail} />
          <MetricCard label="With Instagram" value={withInstagram} />
          <MetricCard label="With Phone" value={withPhone} />
        </div>
      </section>

      <section className="panel">
        <h2>Known Nationals Data</h2>
        <div className="tableWrap">
          <table>
            <thead>
              <tr>
                <th>Fraternity</th>
                <th>National URL</th>
                <th>Email</th>
                <th>Instagram</th>
                <th>Phone</th>
                <th>Address</th>
                <th>URL Provenance</th>
                <th>Email Provenance</th>
                <th>Instagram Provenance</th>
              </tr>
            </thead>
            <tbody>
              {profiles.map((item) => (
                <tr key={item.fraternitySlug}>
                  <td>{item.fraternityName}</td>
                  <td>
                    <a href={item.nationalUrl} target="_blank" rel="noreferrer">
                      {item.nationalUrl}
                    </a>
                  </td>
                  <td>{item.contactEmail ?? <span className="muted">n/a</span>}</td>
                  <td>
                    {item.instagramUrl ? (
                      <a href={item.instagramUrl} target="_blank" rel="noreferrer">
                        {item.instagramUrl}
                      </a>
                    ) : (
                      <span className="muted">n/a</span>
                    )}
                  </td>
                  <td>{item.phone ?? <span className="muted">n/a</span>}</td>
                  <td>{item.addressText ?? <span className="muted">n/a</span>}</td>
                  <td>{item.nationalUrlProvenanceType ? <TagPill label={item.nationalUrlProvenanceType} tone="info" /> : <span className="muted">n/a</span>}</td>
                  <td>{item.contactEmailProvenanceType ? <TagPill label={item.contactEmailProvenanceType} tone="info" /> : <span className="muted">n/a</span>}</td>
                  <td>{item.instagramProvenanceType ? <TagPill label={item.instagramProvenanceType} tone="info" /> : <span className="muted">n/a</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
