import type { Metadata } from "next";

import { APP_VERSION_LABEL } from "@/lib/platform-version";

import "./globals.css";

export const metadata: Metadata = {
  title: `Frat Finder AI ${APP_VERSION_LABEL} Dashboard`,
  description: "LangGraph-native fraternity crawl operations dashboard"
};

const navItems = [
  { href: "/", label: "Overview" },
  { href: "/chapters", label: "Chapters" },
  { href: "/nationals", label: "Nationals" },
  { href: "/runs", label: "Crawl Runs" },
  { href: "/agent-ops", label: "Agent Ops" },
  { href: "/review-items", label: "Review Queue" },
  { href: "/benchmarks", label: "Benchmarks" },
  { href: "/campaigns", label: "Campaigns" },
  { href: "/crm", label: "CRM" },
  { href: "/fraternity-intake", label: "Fraternity Intake" }
];

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <div className="shellGlow shellGlowA" />
        <div className="shellGlow shellGlowB" />
        <div className="shell">
          <aside className="workspaceRail">
            <div className="railCard railBrandCard">
              <p className="eyebrow">Frat Finder AI</p>
              <h1>Operator Console</h1>
              <p className="headerDescription">{APP_VERSION_LABEL} request workers, LangGraph evidence flows, and chapter coverage in one operating surface.</p>
            </div>
            <nav className="railNav" aria-label="Primary">
              {navItems.map((item, index) => (
                <a key={item.href} href={item.href} className="navLink">
                  <span className="navLinkIndex">{String(index + 1).padStart(2, "0")}</span>
                  <span>{item.label}</span>
                </a>
              ))}
            </nav>
            <div className="railCard railNoteCard">
              <p className="eyebrow">Operator Notes</p>
              <p className="railNoteLead">Design for safe writes first.</p>
              <ul className="railChecklist">
                <li>Prefer source-native discovery over broad search.</li>
                <li>Route uncertainty to review, not silent writes.</li>
                <li>Use benchmarks to validate throughput before rollout.</li>
              </ul>
            </div>
          </aside>
          <div className="workspaceContent">
            <header className="header appHeader">
              <div className="headerKicker">
                <p className="eyebrow">Operations Dashboard</p>
                <p className="headerDescription">A modern control room for staged crawl requests, benchmark telemetry, and review-driven quality.</p>
              </div>
              <div className="headerMetaBand">
                <span className="headerMetaChip">Registry-first discovery</span>
                <span className="headerMetaChip">{APP_VERSION_LABEL} request workers</span>
                <span className="headerMetaChip">Search-safe enrichment</span>
                <span className="headerMetaChip">Review-aware writes</span>
              </div>
            </header>
            <main className="mainStack">{children}</main>
          </div>
        </div>
      </body>
    </html>
  );
}
