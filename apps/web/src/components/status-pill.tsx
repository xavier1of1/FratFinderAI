export function StatusPill({ status }: { status: string }) {
  return <span className={`status ${status}`}>{status}</span>;
}
