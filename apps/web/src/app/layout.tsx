import type { Metadata } from "next";
import { Space_Grotesk, IBM_Plex_Mono } from "next/font/google";
import Link from "next/link";

import "./globals.css";

const displayFont = Space_Grotesk({ subsets: ["latin"], variable: "--font-display" });
const monoFont = IBM_Plex_Mono({ subsets: ["latin"], variable: "--font-mono", weight: ["400", "500"] });

export const metadata: Metadata = {
  title: "Frat Finder AI Dashboard",
  description: "Source-aware chapter discovery operations dashboard"
};

const navItems = [
  { href: "/", label: "Overview" },
  { href: "/chapters", label: "Chapters" },
  { href: "/runs", label: "Crawl Runs" },
  { href: "/review-items", label: "Review Queue" }
];

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${displayFont.variable} ${monoFont.variable}`}>
      <body>
        <div className="shell">
          <header className="header">
            <div>
              <p className="eyebrow">Frat Finder AI</p>
              <h1>Operator Dashboard</h1>
            </div>
            <nav>
              {navItems.map((item) => (
                <Link key={item.href} href={item.href} className="navLink">
                  {item.label}
                </Link>
              ))}
            </nav>
          </header>
          <main>{children}</main>
        </div>
      </body>
    </html>
  );
}