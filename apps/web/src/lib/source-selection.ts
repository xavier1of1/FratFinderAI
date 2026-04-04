import type { FraternityDiscoveryCandidate } from "@/lib/types";
import type { FraternitySourceDiscoveryResult } from "@/lib/fraternity-discovery";

export interface SourceQualityEvaluation {
  score: number;
  isWeak: boolean;
  isBlocked: boolean;
  reasons: string[];
}

const BLOCKED_HOSTS = new Set([
  "wikipedia.org",
  "www.wikipedia.org",
  "reddit.com",
  "www.reddit.com",
  "facebook.com",
  "www.facebook.com",
  "instagram.com",
  "www.instagram.com",
  "linkedin.com",
  "www.linkedin.com",
  "x.com",
  "twitter.com",
  "stackoverflow.com",
  "stackexchange.com",
  "github.com",
  "medium.com",
  "quora.com",
  "wiktionary.org"
]);

const POSITIVE_URL_MARKERS = [
  "chapter",
  "chapters",
  "chapter-directory",
  "find-a-chapter",
  "findachapter",
  "our-chapters",
  "locations",
  "locator",
  "map",
  "undergraduate"
];

const WEAK_URL_MARKERS = [
  "alumni",
  "alumni-groups",
  "alumnigroups",
  "member",
  "members",
  "memberhub",
  "portal",
  "login",
  "account",
  "donate"
];

const POSITIVE_TEXT_MARKERS = [
  "chapter directory",
  "find a chapter",
  "our chapters",
  "chapter map",
  "undergraduate",
  "active chapters",
  "chapter experience"
];

function hostIsBlocked(hostname: string): boolean {
  const normalized = hostname.toLowerCase().replace(/\.$/, "");
  if (BLOCKED_HOSTS.has(normalized)) {
    return true;
  }
  for (const blocked of BLOCKED_HOSTS) {
    if (normalized.endsWith(`.${blocked}`)) {
      return true;
    }
  }
  return false;
}

export function evaluateSourceUrl(url: string | null | undefined): SourceQualityEvaluation {
  if (!url) {
    return {
      score: 0,
      isWeak: true,
      isBlocked: false,
      reasons: ["missing_url"]
    };
  }

  try {
    const parsed = new URL(url);
    const normalized = `${parsed.hostname}${parsed.pathname}`.toLowerCase();
    const reasons: string[] = [];
    let score = 0.55;
    const isBlocked = hostIsBlocked(parsed.hostname);
    if (isBlocked) {
      score -= 0.7;
      reasons.push("blocked_host");
    }

    const positiveHits = POSITIVE_URL_MARKERS.filter((marker) => normalized.includes(marker));
    if (positiveHits.length > 0) {
      score += Math.min(0.35, positiveHits.length * 0.08);
      reasons.push(...positiveHits.map((marker) => `positive:${marker}`));
    }

    const weakHits = WEAK_URL_MARKERS.filter((marker) => normalized.includes(marker));
    if (weakHits.length > 0) {
      score -= Math.min(0.75, weakHits.length * 0.24);
      reasons.push(...weakHits.map((marker) => `weak:${marker}`));
    }

    const path = parsed.pathname.replace(/\/+$/, "");
    if (!path || path === "") {
      score -= 0.12;
      reasons.push("generic_root_path");
    } else if (path.split("/").filter(Boolean).length >= 2) {
      score += 0.06;
      reasons.push("deeper_path");
    }

    const boundedScore = Math.max(0, Math.min(1, score));
    return {
      score: boundedScore,
      isWeak: isBlocked || boundedScore < 0.45 || weakHits.length > 0,
      isBlocked,
      reasons
    };
  } catch {
    return {
      score: 0,
      isWeak: true,
      isBlocked: false,
      reasons: ["invalid_url"]
    };
  }
}

function scoreCandidateText(candidate: FraternityDiscoveryCandidate): number {
  const combined = `${candidate.title} ${candidate.snippet}`.toLowerCase();
  let score = 0;
  for (const marker of POSITIVE_TEXT_MARKERS) {
    if (combined.includes(marker)) {
      score += 0.06;
    }
  }
  return Math.min(0.24, score);
}

function scoreCandidate(candidate: FraternityDiscoveryCandidate): number {
  const quality = evaluateSourceUrl(candidate.url);
  return Number(candidate.score ?? 0) + quality.score + scoreCandidateText(candidate);
}

export function pickBestDiscoveryCandidate(
  candidates: FraternityDiscoveryCandidate[],
  currentUrl?: string | null
): FraternityDiscoveryCandidate | null {
  const current = currentUrl ? currentUrl.replace(/\/+$/, "") : null;
  const ranked = [...candidates]
    .filter((candidate) => candidate.url.replace(/\/+$/, "") !== current)
    .filter((candidate) => !evaluateSourceUrl(candidate.url).isBlocked)
    .sort((left, right) => scoreCandidate(right) - scoreCandidate(left));
  return ranked[0] ?? null;
}

export function optimizeDiscoveredSource(
  discovery: FraternitySourceDiscoveryResult
): FraternitySourceDiscoveryResult {
  const currentQuality = discovery.sourceQuality
    ? {
        score: Number(discovery.sourceQuality.score ?? 0),
        isWeak: Boolean(discovery.sourceQuality.isWeak ?? true),
        isBlocked: Boolean(discovery.sourceQuality.isBlocked ?? false),
        reasons: Array.isArray(discovery.sourceQuality.reasons) ? discovery.sourceQuality.reasons : []
      }
    : evaluateSourceUrl(discovery.selectedUrl);

  const fallbackCandidate = pickBestDiscoveryCandidate(discovery.candidates, discovery.selectedUrl);

  if (!fallbackCandidate) {
    return {
      ...discovery,
      sourceQuality: currentQuality
    };
  }

  const fallbackQuality = evaluateSourceUrl(fallbackCandidate.url);
  const currentComposite = Number(discovery.selectedConfidence ?? 0) + currentQuality.score;
  const fallbackComposite = scoreCandidate(fallbackCandidate);

  if (!currentQuality.isWeak && fallbackComposite <= currentComposite + 0.12) {
    return {
      ...discovery,
      sourceQuality: currentQuality
    };
  }

  if (fallbackComposite <= currentComposite) {
    return {
      ...discovery,
      sourceQuality: currentQuality
    };
  }

  const nextConfidence = Math.max(Number(discovery.selectedConfidence ?? 0), Number(fallbackCandidate.score ?? 0), fallbackQuality.score);

  return {
    ...discovery,
    selectedUrl: fallbackCandidate.url,
    selectedConfidence: Math.min(0.99, Number(nextConfidence.toFixed(2))),
    confidenceTier: nextConfidence >= 0.8 ? "high" : nextConfidence >= 0.6 ? "medium" : "low",
    sourceProvenance: fallbackCandidate.provider === "verified_registry" ? "verified_registry" : fallbackCandidate.provider === "existing_source" ? "existing_source" : "search",
    sourceQuality: fallbackQuality,
    selectedCandidateRationale: "optimized_source_selection",
    fallbackReason: discovery.selectedUrl && discovery.selectedUrl !== fallbackCandidate.url
      ? `upgraded_source_selection:${currentQuality.reasons.join("|") || "quality"}`
      : discovery.fallbackReason,
    resolutionTrace: [
      ...discovery.resolutionTrace,
      {
        step: "optimized_source_selection",
        previousUrl: discovery.selectedUrl,
        previousQualityScore: currentQuality.score,
        nextUrl: fallbackCandidate.url,
        nextQualityScore: fallbackQuality.score,
        candidateScore: fallbackCandidate.score
      }
    ]
  };
}
