export function TagPill({ label, tone = "neutral" }: { label: string; tone?: "neutral" | "info" | "warning" }) {
  return <span className={`tag ${tone}`}>{label}</span>;
}
