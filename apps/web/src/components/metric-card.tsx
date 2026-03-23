export function MetricCard({ label, value }: { label: string; value: string | number }) {
  return (
    <article className="metricCard">
      <p className="metricLabel">{label}</p>
      <p className="metricValue">{value}</p>
    </article>
  );
}