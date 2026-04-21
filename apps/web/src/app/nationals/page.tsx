import { MetricCard } from "@/components/metric-card";
import { PageIntro } from "@/components/page-intro";
import { TagPill } from "@/components/tag-pill";
import { getNationalProfileCounts, listNationalProfiles } from "@/lib/repositories/nationals-profile-repository";
import type { NationalProfile } from "@/lib/types";

export default async function NationalsPage() {
  const [profiles, counts] = await Promise.all([
    listNationalProfiles(250) as Promise<NationalProfile[]>,
    getNationalProfileCounts()
  ]);

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Nationals"
        title="First-class nationals profiles and contact truth"
        description="Use this page to inspect what the system knows about each national organization separately from chapter rows. Generic nationals contact belongs here unless the page is explicitly chapter-specific."
        meta={[`${counts.total} organizations`, `${counts.withEmail} with email`, `${counts.withInstagram} with Instagram`, `${counts.withPhone} with phone`]}
      />

      <section className="panel">
        <h2>Nationals Coverage</h2>
        <p className="sectionDescription">This surface is the new source of truth for national-level contact and provenance so chapter rows do not silently inherit generic HQ data. The table below shows the first 250 profiles while the KPIs reflect full-platform totals.</p>
        <div className="metrics">
          <MetricCard label="Profiles" value={counts.total} />
          <MetricCard label="With Email" value={counts.withEmail} />
          <MetricCard label="With Instagram" value={counts.withInstagram} />
          <MetricCard label="With Phone" value={counts.withPhone} />
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
