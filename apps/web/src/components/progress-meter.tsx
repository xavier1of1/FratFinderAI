export function ProgressMeter({
  label,
  value,
  total,
  hint
}: {
  label: string;
  value: number;
  total: number;
  hint?: string;
}) {
  const safeTotal = total <= 0 ? 1 : total;
  const percentage = Math.max(0, Math.min(100, (value / safeTotal) * 100));

  return (
    <div className="progressMeter">
      <div className="progressMeterHeader">
        <span>{label}</span>
        <strong>{Math.round(percentage)}%</strong>
      </div>
      <div className="progressMeterTrack" aria-hidden="true">
        <div className="progressMeterFill" style={{ width: `${percentage}%` }} />
      </div>
      <div className="progressMeterMeta">
        <span>
          {value} / {total}
        </span>
        <span>{hint ?? "live"}</span>
      </div>
    </div>
  );
}
