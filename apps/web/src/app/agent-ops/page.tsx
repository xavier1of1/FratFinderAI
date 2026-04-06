import { MetricCard } from "@/components/metric-card";
import { PageIntro } from "@/components/page-intro";
import { StatusPill } from "@/components/status-pill";
import { TagPill } from "@/components/tag-pill";
import { fetchFromApi } from "@/lib/api-client";
import { instagramHandleFromUrl } from "@/lib/social";
import type { AgentOpsSummary, ChapterEvidence, ChapterSearchRun, OpsAlert, ProvisionalChapter, RequestGraphRun } from "@/lib/types";

interface AgentOpsPayload {
  summary: AgentOpsSummary;
  graphRuns: RequestGraphRun[];
  provisionalChapters: ProvisionalChapter[];
  evidence: ChapterEvidence[];
  chapterSearchRuns: ChapterSearchRun[];
  opsAlerts: OpsAlert[];
}

export default async function AgentOpsPage() {
  const data = await fetchFromApi<AgentOpsPayload>("/api/agent-ops?limit=75");
  const running = data.graphRuns.filter((item) => item.status === "running").length;
  const paused = data.graphRuns.filter((item) => item.status === "paused").length;
  const provisionalOpen = data.provisionalChapters.filter((item) => item.status === "provisional").length;
  const queueClear = data.summary.requestQueueQueued === 0 && data.summary.requestQueueRunning === 0;
  const latestChapterSearch = data.chapterSearchRuns[0] ?? null;

  return (
    <div className="sectionStack">
      <PageIntro
        eyebrow="Agent Ops"
        title="V3.0.1 LangGraph execution, queue health, and evidence"
        description="Use this console to inspect V3 request graph runs, verify the worker queue is draining cleanly, and audit evidence before anything becomes canonical chapter data."
        meta={[
          `${data.graphRuns.length} graph runs`,
          `${running} running`,
          `${paused} paused`,
          `${provisionalOpen} provisional chapters`,
          queueClear ? "request queue clear" : "request queue active"
        ]}
      />

      <section className="panel">
        <h2>Runtime Health</h2>
        <p className="sectionDescription">These counters are the fastest way to see whether the V3 worker loop is healthy or backing up.</p>
        <div className="metrics">
          <MetricCard label="Queued Requests" value={data.summary.requestQueueQueued} />
          <MetricCard label="Running Requests" value={data.summary.requestQueueRunning} />
          <MetricCard label="Awaiting Confirmation" value={data.summary.requestAwaitingConfirmation} />
          <MetricCard label="Completed Requests" value={data.summary.requestCompleted} />
          <MetricCard label="Queued Field Jobs" value={data.summary.fieldJobsQueued} />
          <MetricCard label="Actionable Field Jobs" value={data.summary.fieldJobsActionable} />
          <MetricCard label="Running Field Jobs" value={data.summary.fieldJobsRunning} />
          <MetricCard label="Deferred Field Jobs" value={data.summary.fieldJobsDeferred} />
          <MetricCard label="Blocked Invalid Jobs" value={data.summary.fieldJobsBlockedInvalid} />
          <MetricCard label="Blocked Repair Jobs" value={data.summary.fieldJobsBlockedRepairable} />
          <MetricCard label="Queued Repair Jobs" value={data.summary.chapterRepairQueued} />
          <MetricCard label="Running Repair Jobs" value={data.summary.chapterRepairRunning} />
          <MetricCard label="Completed Repair Jobs" value={data.summary.chapterRepairCompleted} />
          <MetricCard label="Historical Reconciliations" value={data.summary.chapterRepairHistoricalReconciled} />
          <MetricCard label="Terminal No Signal" value={data.summary.fieldJobsTerminalNoSignal} />
          <MetricCard label="Review Required" value={data.summary.fieldJobsReviewRequired} />
          <MetricCard label="Auto Written" value={data.summary.fieldJobsUpdated} />
          <MetricCard label="Evidence In Review" value={data.summary.evidenceReview} />
          <MetricCard label="Evidence Ready To Write" value={data.summary.evidenceWrite} />
          <MetricCard label="Open Ops Alerts" value={data.summary.opsAlertsOpen} />
          <MetricCard label="Critical Ops Alerts" value={data.summary.opsAlertsCritical} />
          <MetricCard label="Warning Ops Alerts" value={data.summary.opsAlertsWarning} />
          <MetricCard label="Resolved Alerts 24h" value={data.summary.opsAlertsResolvedLast24h} />
          <MetricCard label="Oldest Open Alert" value={`${data.summary.opsAlertsOldestOpenMinutes}m`} />
          <MetricCard label="Oldest Open Provisional" value={`${data.summary.provisionalOldestOpenHours}h`} />
        </div>
        <p className="muted">
          Queue status: {queueClear ? "No request-level bottleneck detected." : "Requests are still queued or running, so the queue is active."}
        </p>
      </section>

      <section className="panel">
        <h2>Chapter Search Core</h2>
        <p className="sectionDescription">This is the new V3 chapter-discovery surface: national and institutional follow behavior, canonical vs provisional creation, and rejected candidate reasons.</p>
        <div className="metrics">
          <MetricCard label="Chapter Search Runs" value={data.summary.chapterSearchRuns} />
          <MetricCard label="Canonical Created" value={data.summary.chapterSearchCanonical} />
          <MetricCard label="Provisional Created" value={data.summary.chapterSearchProvisional} />
          <MetricCard label="External Targets Skipped" value={data.summary.chapterSearchChapterOwnedSkipped} />
          <MetricCard label="Invalid Entities" value={data.summary.chapterValidityInvalid} />
          <MetricCard label="Repairable Entities" value={data.summary.chapterValidityRepairable} />
          <MetricCard label="Blocked Invalid Jobs" value={data.summary.chapterValidityBlockedInvalid} />
          <MetricCard label="Blocked Repair Jobs" value={data.summary.chapterValidityBlockedRepairable} />
          <MetricCard label="Provisional Review" value={data.summary.provisionalReview} />
          <MetricCard label="Provisional Rejected" value={data.summary.provisionalRejected} />
        </div>
        <p className="muted">
          Latest: {latestChapterSearch ? `${latestChapterSearch.sourceSlug ?? "unknown source"} / ${latestChapterSearch.sourceClass ?? "unknown class"} / ${latestChapterSearch.coverageState ?? "unknown coverage"}` : "No chapter-search runs yet."}
        </p>
        <div className="tableWrap">
          <table>
            <thead>
              <tr>
                <th>Run</th>
                <th>Source</th>
                <th>Status</th>
                <th>Class</th>
                <th>Coverage</th>
                <th>Canonical</th>
                <th>Provisional</th>
                <th>National</th>
                <th>Institutional</th>
                <th>Skipped Sites</th>
                <th>Rejected</th>
                <th>Invalid</th>
                <th>Repairable</th>
                <th>Saturated</th>
                <th>Wall Time</th>
              </tr>
            </thead>
            <tbody>
              {data.chapterSearchRuns.map((run) => {
                const topRejection = Object.entries(run.rejectionReasonCounts ?? {}).sort((left, right) => right[1] - left[1])[0] ?? null;
                return (
                  <tr key={run.id}>
                    <td>{run.id}</td>
                    <td>{run.sourceSlug ?? <span className="muted">n/a</span>}</td>
                    <td><StatusPill status={run.status} /></td>
                    <td>{run.sourceClass ?? <span className="muted">n/a</span>}</td>
                    <td>{run.coverageState ? <TagPill label={run.coverageState} tone="info" /> : <span className="muted">n/a</span>}</td>
                    <td>{run.canonicalChaptersCreated}</td>
                    <td>{run.provisionalChaptersCreated}</td>
                    <td>{run.nationalTargetsFollowed}</td>
                    <td>{run.institutionalTargetsFollowed}</td>
                    <td>{run.chapterOwnedTargetsSkipped}</td>
                    <td>{topRejection ? `${topRejection[0]} (${topRejection[1]})` : <span className="muted">none</span>}</td>
                    <td>{run.invalidCount}</td>
                    <td>{run.repairableCount}</td>
                    <td>{run.sourceInvaliditySaturated ? <TagPill label="saturated" tone="warning" /> : <span className="muted">no</span>}</td>
                    <td>{Math.round(run.chapterSearchWallTimeMs / 1000)}s</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <h2>Request Graph Runs</h2>
        <p className="sectionDescription">Every V3 request worker execution is checkpointed here so runtime ownership is visible outside the crawler logs.</p>
        <div className="tableWrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Request</th>
                <th>Fraternity</th>
                <th>Status</th>
                <th>Runtime</th>
                <th>Active Node</th>
                <th>Worker</th>
                <th>Source</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {data.graphRuns.map((run) => (
                <tr key={run.id}>
                  <td>{run.id}</td>
                  <td>{run.requestId}</td>
                  <td>{run.fraternityName ?? run.fraternitySlug ?? <span className="muted">n/a</span>}</td>
                  <td>
                    <StatusPill status={run.status} />
                  </td>
                  <td>{run.runtimeMode ? <TagPill label={run.runtimeMode} tone="info" /> : <span className="muted">n/a</span>}</td>
                  <td>{run.activeNode ?? <span className="muted">n/a</span>}</td>
                  <td>{run.workerId}</td>
                  <td>{run.sourceSlug ?? <span className="muted">n/a</span>}</td>
                  <td>{new Date(run.createdAt).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <h2>Provisional Chapters</h2>
        <p className="sectionDescription">Broader-web chapter discoveries live here until they are promoted by strong official evidence or resolved by review.</p>
        <p className="muted">
          Open: {data.summary.provisionalOpen} | Promoted: {data.summary.provisionalPromoted} | Review: {data.summary.provisionalReview} | Rejected: {data.summary.provisionalRejected} | Oldest Open Age: {data.summary.provisionalOldestOpenHours}h
        </p>
        <div className="tableWrap">
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Fraternity</th>
                <th>University</th>
                <th>Status</th>
                <th>Website</th>
                <th>Email</th>
                <th>Instagram</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {data.provisionalChapters.map((chapter) => (
                <tr key={chapter.id}>
                  <td>{chapter.name}</td>
                  <td>{chapter.fraternitySlug ?? <span className="muted">n/a</span>}</td>
                  <td>{chapter.universityName ?? <span className="muted">n/a</span>}</td>
                  <td>{chapter.status ? <TagPill label={chapter.status} tone="warning" /> : <span className="muted">n/a</span>}</td>
                  <td>{chapter.websiteUrl ?? <span className="muted">n/a</span>}</td>
                  <td>{chapter.contactEmail ?? <span className="muted">n/a</span>}</td>
                  <td>
                    {chapter.instagramUrl ? (
                      <a href={chapter.instagramUrl} target="_blank" rel="noreferrer">
                        @{instagramHandleFromUrl(chapter.instagramUrl) ?? "profile"}
                      </a>
                    ) : (
                      <span className="muted">n/a</span>
                    )}
                  </td>
                  <td>{new Date(chapter.updatedAt).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <h2>Ops Alerts</h2>
        <p className="sectionDescription">Operational failures and saturation issues should be visible here without digging through raw logs or database tables.</p>
        <p className="muted">
          Open: {data.summary.opsAlertsOpen} | Critical: {data.summary.opsAlertsCritical} | Warning: {data.summary.opsAlertsWarning} | Resolved 24h: {data.summary.opsAlertsResolvedLast24h}
        </p>
        <div className="tableWrap">
          <table>
            <thead>
              <tr>
                <th>Scope</th>
                <th>Type</th>
                <th>Severity</th>
                <th>Status</th>
                <th>Source</th>
                <th>Message</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {data.opsAlerts.map((alert) => (
                <tr key={alert.id}>
                  <td>{alert.alertScope}</td>
                  <td>{alert.alertType}</td>
                  <td>{alert.severity ? <TagPill label={alert.severity} tone={alert.severity === "info" ? "info" : "warning"} /> : <span className="muted">n/a</span>}</td>
                  <td>{alert.status ? <StatusPill status={alert.status} /> : <span className="muted">n/a</span>}</td>
                  <td>{alert.sourceSlug ?? <span className="muted">n/a</span>}</td>
                  <td>{alert.message}</td>
                  <td>{new Date(alert.createdAt).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <h2>Recent Evidence</h2>
        <p className="sectionDescription">This is the candidate ledger behind V3 writes, review routing, and later reinforcement signals.</p>
        <p className="muted">
          Total ledger rows: {data.summary.evidenceTotal} | Review: {data.summary.evidenceReview} | Ready to write: {data.summary.evidenceWrite}
        </p>
        <div className="tableWrap">
          <table>
            <thead>
              <tr>
                <th>Chapter</th>
                <th>Field</th>
                <th>Candidate</th>
                <th>Status</th>
                <th>Trust</th>
                <th>Confidence</th>
                <th>Provider</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {data.evidence.map((item) => (
                <tr key={item.id}>
                  <td>{item.chapterSlug ?? <span className="muted">n/a</span>}</td>
                  <td>{item.fieldName}</td>
                  <td>{item.candidateValue ?? <span className="muted">n/a</span>}</td>
                  <td>{item.evidenceStatus ? <TagPill label={item.evidenceStatus} tone="info" /> : <span className="muted">n/a</span>}</td>
                  <td>{item.trustTier ?? <span className="muted">n/a</span>}</td>
                  <td>{item.confidence !== null ? item.confidence.toFixed(2) : <span className="muted">n/a</span>}</td>
                  <td>{item.provider ?? <span className="muted">n/a</span>}</td>
                  <td>{new Date(item.createdAt).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
