"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";

import { MetricCard } from "@/components/metric-card";
import { defaultCrmMessage, defaultCrmSubject } from "@/lib/crm";
import type { ApiEnvelope } from "@/lib/api-envelope";
import type { CrmCampaign, CrmChannel, CrmDispatchMode, CrmRecipientStatus } from "@/lib/types";

type CampaignFormState = {
  name: string;
  channel: CrmChannel;
  fraternitySlug: string;
  state: string;
  chapterStatus: "active" | "inactive" | "all";
  search: string;
  limit: number;
  subjectTemplate: string;
  messageTemplate: string;
};

const INITIAL_FORM: CampaignFormState = {
  name: "",
  channel: "email",
  fraternitySlug: "",
  state: "",
  chapterStatus: "active",
  search: "",
  limit: 50,
  subjectTemplate: defaultCrmSubject("email") ?? "",
  messageTemplate: defaultCrmMessage("email")
};

function formatTimestamp(value: string | null): string {
  if (!value) {
    return "n/a";
  }
  return new Date(value).toLocaleString();
}

async function fetchCampaigns(): Promise<CrmCampaign[]> {
  const response = await fetch("/api/crm/campaigns?limit=100", { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<CrmCampaign[]>;
  if (!response.ok || !payload.success) {
    throw new Error(payload.success ? `Failed to fetch CRM campaigns (${response.status})` : payload.error.message);
  }
  return payload.data;
}

async function fetchCampaignDetail(id: string): Promise<CrmCampaign> {
  const response = await fetch(`/api/crm/campaigns/${id}`, { cache: "no-store" });
  const payload = (await response.json()) as ApiEnvelope<CrmCampaign>;
  if (!response.ok || !payload.success) {
    throw new Error(payload.success ? `Failed to fetch CRM campaign (${response.status})` : payload.error.message);
  }
  return payload.data;
}

export function CrmDashboard({
  initialCampaigns,
  fraternityOptions,
  stateOptions
}: {
  initialCampaigns: CrmCampaign[];
  fraternityOptions: string[];
  stateOptions: string[];
}) {
  const [campaigns, setCampaigns] = useState<CrmCampaign[]>(initialCampaigns);
  const [selectedId, setSelectedId] = useState<string | null>(initialCampaigns[0]?.id ?? null);
  const [selectedCampaign, setSelectedCampaign] = useState<CrmCampaign | null>(initialCampaigns[0] ?? null);
  const [form, setForm] = useState<CampaignFormState>(INITIAL_FORM);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isDispatching, setIsDispatching] = useState(false);
  const [copyFeedback, setCopyFeedback] = useState<string | null>(null);

  useEffect(() => {
    setForm((current) => ({
      ...current,
      subjectTemplate: current.channel === "email" ? current.subjectTemplate || (defaultCrmSubject("email") ?? "") : "",
      messageTemplate: current.messageTemplate || defaultCrmMessage(current.channel)
    }));
  }, []);

  useEffect(() => {
    if (!selectedId) {
      setSelectedCampaign(campaigns[0] ?? null);
      return;
    }
    const matching = campaigns.find((item) => item.id === selectedId) ?? null;
    setSelectedCampaign(matching);
  }, [campaigns, selectedId]);

  const selectedSummary = useMemo(() => selectedCampaign ?? campaigns[0] ?? null, [campaigns, selectedCampaign]);

  async function refreshCampaignList(options?: { keepSelected?: boolean }) {
    setIsRefreshing(true);
    try {
      const next = await fetchCampaigns();
      setCampaigns(next);
      if (!options?.keepSelected) {
        setSelectedId((current) => current ?? next[0]?.id ?? null);
      }
      if (selectedId) {
        const detail = next.find((item) => item.id === selectedId);
        if (detail) {
          setSelectedCampaign(await fetchCampaignDetail(detail.id));
        }
      }
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    } finally {
      setIsRefreshing(false);
    }
  }

  async function createCampaign(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setIsSubmitting(true);
    setErrorMessage(null);
    try {
      const payload = {
        name: form.name.trim() || `${form.channel === "email" ? "Email" : "Instagram"} outreach ${new Date().toLocaleString()}`,
        channel: form.channel,
        deliveryMode: form.channel === "email" ? "outlook" : "operator",
        subjectTemplate: form.channel === "email" ? form.subjectTemplate : null,
        messageTemplate: form.messageTemplate,
        filters: {
          fraternitySlug: form.fraternitySlug || null,
          state: form.state || null,
          chapterStatus: form.chapterStatus,
          search: form.search || null,
          limit: form.limit
        }
      };

      const response = await fetch("/api/crm/campaigns", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const envelope = (await response.json()) as ApiEnvelope<CrmCampaign>;
      if (!response.ok || !envelope.success) {
        throw new Error(envelope.success ? `Failed to create campaign (${response.status})` : envelope.error.message);
      }

      setCampaigns((current) => [envelope.data, ...current]);
      setSelectedId(envelope.data.id);
      setSelectedCampaign(envelope.data);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    } finally {
      setIsSubmitting(false);
    }
  }

  async function dispatchCampaign(mode: CrmDispatchMode) {
    if (!selectedSummary) {
      return;
    }
    setIsDispatching(true);
    setErrorMessage(null);
    try {
      const response = await fetch(`/api/crm/campaigns/${selectedSummary.id}/dispatch`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode })
      });
      const envelope = (await response.json()) as ApiEnvelope<{
        campaign: CrmCampaign;
        processed: number;
        drafted: number;
        sent: number;
        failed: number;
      }>;
      if (!response.ok || !envelope.success) {
        throw new Error(envelope.success ? `Failed to dispatch campaign (${response.status})` : envelope.error.message);
      }

      const nextCampaign = envelope.data.campaign;
      setCampaigns((current) => current.map((item) => (item.id === nextCampaign.id ? nextCampaign : item)));
      setSelectedCampaign(nextCampaign);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    } finally {
      setIsDispatching(false);
    }
  }

  async function updateRecipientStatus(recipientId: string, status: CrmRecipientStatus) {
    setErrorMessage(null);
    try {
      const response = await fetch(`/api/crm/recipients/${recipientId}/status`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status })
      });
      const envelope = (await response.json()) as ApiEnvelope<{ campaignId: string }>;
      if (!response.ok || !envelope.success) {
        throw new Error(envelope.success ? `Failed to update recipient (${response.status})` : envelope.error.message);
      }
      if (selectedSummary) {
        const refreshed = await fetchCampaignDetail(selectedSummary.id);
        setCampaigns((current) => current.map((item) => (item.id === refreshed.id ? refreshed : item)));
        setSelectedCampaign(refreshed);
      }
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : String(error));
    }
  }

  async function copyMessage(value: string) {
    await navigator.clipboard.writeText(value);
    setCopyFeedback("Message copied.");
    window.setTimeout(() => setCopyFeedback(null), 1500);
  }

  return (
    <div className="sectionStack">
      <section className="panel heroPanel">
        <h2>Outbound CRM</h2>
        <p className="sectionDescription">
          Email campaigns can draft or send through Outlook on this workstation. Instagram campaigns run as a guided outreach queue so operators can open the profile, paste the prepared message, and track completion.
        </p>
        <div style={{ display: "grid", gap: "1rem", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))" }}>
          <MetricCard label="Campaigns" value={campaigns.length} />
          <MetricCard label="Ready Recipients" value={campaigns.reduce((sum, item) => sum + item.queuedCount, 0)} />
          <MetricCard label="Drafted" value={campaigns.reduce((sum, item) => sum + item.draftedCount, 0)} />
          <MetricCard label="Sent" value={campaigns.reduce((sum, item) => sum + item.sentCount, 0)} />
        </div>
      </section>

      <section className="panel">
        <h2>Create Campaign</h2>
        <p className="sectionDescription">Filter chapter contacts down to the audience you want, then generate personalized email or Instagram outreach from live chapter data.</p>
        <form onSubmit={createCampaign} style={{ display: "grid", gap: "1rem" }}>
          <div style={{ display: "grid", gap: "1rem", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))" }}>
            <label>
              <span>Name</span>
              <input value={form.name} onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))} placeholder="Spring outreach wave" />
            </label>
            <label>
              <span>Channel</span>
              <select
                value={form.channel}
                onChange={(event) => {
                  const nextChannel = event.target.value as CrmChannel;
                  setForm((current) => ({
                    ...current,
                    channel: nextChannel,
                    subjectTemplate: nextChannel === "email" ? defaultCrmSubject("email") ?? "" : "",
                    messageTemplate: defaultCrmMessage(nextChannel)
                  }));
                }}
              >
                <option value="email">Email</option>
                <option value="instagram">Instagram</option>
              </select>
            </label>
            <label>
              <span>Fraternity</span>
              <select value={form.fraternitySlug} onChange={(event) => setForm((current) => ({ ...current, fraternitySlug: event.target.value }))}>
                <option value="">All fraternities</option>
                {fraternityOptions.map((option) => (
                  <option key={option} value={option}>{option}</option>
                ))}
              </select>
            </label>
            <label>
              <span>State</span>
              <select value={form.state} onChange={(event) => setForm((current) => ({ ...current, state: event.target.value }))}>
                <option value="">All states</option>
                {stateOptions.map((option) => (
                  <option key={option} value={option}>{option}</option>
                ))}
              </select>
            </label>
            <label>
              <span>Status</span>
              <select value={form.chapterStatus} onChange={(event) => setForm((current) => ({ ...current, chapterStatus: event.target.value as CampaignFormState["chapterStatus"] }))}>
                <option value="active">Active only</option>
                <option value="inactive">Inactive only</option>
                <option value="all">All statuses</option>
              </select>
            </label>
            <label>
              <span>Recipient limit</span>
              <input type="number" min={1} max={500} value={form.limit} onChange={(event) => setForm((current) => ({ ...current, limit: Number(event.target.value) || 1 }))} />
            </label>
          </div>
          <label>
            <span>Search</span>
            <input value={form.search} onChange={(event) => setForm((current) => ({ ...current, search: event.target.value }))} placeholder="University, chapter, or campus keyword" />
          </label>
          {form.channel === "email" ? (
            <label>
              <span>Subject template</span>
              <input value={form.subjectTemplate} onChange={(event) => setForm((current) => ({ ...current, subjectTemplate: event.target.value }))} placeholder="Quick note for {chapterName} at {universityName}" />
            </label>
          ) : null}
          <label>
            <span>Message template</span>
            <textarea
              rows={10}
              value={form.messageTemplate}
              onChange={(event) => setForm((current) => ({ ...current, messageTemplate: event.target.value }))}
              placeholder="Use tokens like {chapterName}, {universityName}, {fraternityName}, {city}, and {state}"
            />
          </label>
          <div style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap" }}>
            <button type="submit" className="buttonPrimaryAuto" disabled={isSubmitting}>
              {isSubmitting ? "Creating..." : "Create Campaign"}
            </button>
            <button type="button" className="buttonSecondary" disabled={isRefreshing} onClick={() => void refreshCampaignList({ keepSelected: true })}>
              Refresh
            </button>
          </div>
          {errorMessage ? <p className="muted">{errorMessage}</p> : null}
          {copyFeedback ? <p className="muted">{copyFeedback}</p> : null}
        </form>
      </section>

      <div style={{ display: "grid", gap: "1rem", gridTemplateColumns: "minmax(280px, 360px) minmax(0, 1fr)" }}>
        <article className="panel">
          <h2>Campaigns</h2>
          <p className="sectionDescription">Newest campaigns first. Pick one to review recipients and dispatch it.</p>
          {campaigns.length === 0 ? (
            <p className="muted">No CRM campaigns yet.</p>
          ) : (
            <div style={{ display: "grid", gap: "0.75rem" }}>
              {campaigns.map((campaign) => (
                <button
                  key={campaign.id}
                  type="button"
                  className={`buttonSecondary${campaign.id === selectedSummary?.id ? " isActiveFilter" : ""}`}
                  style={{ justifyContent: "space-between", textAlign: "left" }}
                  onClick={async () => {
                    setSelectedId(campaign.id);
                    setSelectedCampaign(await fetchCampaignDetail(campaign.id));
                  }}
                >
                  <span>
                    <strong>{campaign.name}</strong>
                    <br />
                    <span className="muted">{campaign.channel} · {campaign.recipientCount} recipients · {campaign.status}</span>
                  </span>
                </button>
              ))}
            </div>
          )}
        </article>

        <article className="panel">
          {!selectedSummary ? (
            <p className="muted">Create or select a campaign to inspect recipients.</p>
          ) : (
            <div style={{ display: "grid", gap: "1rem" }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: "1rem", flexWrap: "wrap" }}>
                <div>
                  <h2>{selectedSummary.name}</h2>
                  <p className="sectionDescription">
                    {selectedSummary.channel === "email"
                      ? "Dispatch Outlook drafts for review or send the full campaign live from this workstation."
                      : "Work this Instagram queue operator-side: copy the prepared message, open the target profile, and mark outreach complete."}
                  </p>
                  <p className="muted">
                    Created {formatTimestamp(selectedSummary.createdAt)} · launched {formatTimestamp(selectedSummary.launchedAt)} · status {selectedSummary.status}
                  </p>
                </div>
                <div style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap", alignSelf: "flex-start" }}>
                  {selectedSummary.channel === "email" ? (
                    <>
                      <button type="button" className="buttonPrimaryAuto" disabled={isDispatching || selectedSummary.queuedCount === 0} onClick={() => void dispatchCampaign("draft")}>
                        {isDispatching ? "Working..." : "Draft in Outlook"}
                      </button>
                      <button type="button" className="buttonSecondary" disabled={isDispatching || selectedSummary.queuedCount === 0} onClick={() => void dispatchCampaign("send")}>
                        Send Live
                      </button>
                    </>
                  ) : null}
                </div>
              </div>

              <div style={{ display: "grid", gap: "1rem", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))" }}>
                <MetricCard label="Recipients" value={selectedSummary.recipientCount} />
                <MetricCard label="Queued" value={selectedSummary.queuedCount} />
                <MetricCard label="Drafted" value={selectedSummary.draftedCount} />
                <MetricCard label="Sent" value={selectedSummary.sentCount} />
                <MetricCard label="Failed" value={selectedSummary.failedCount} />
              </div>

              <div style={{ overflowX: "auto" }}>
                <table className="dataTable">
                  <thead>
                    <tr>
                      <th>Chapter</th>
                      <th>Campus</th>
                      <th>Contact</th>
                      <th>Status</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {selectedSummary.recipients.map((recipient) => (
                      <tr key={recipient.id}>
                        <td>
                          <strong>{recipient.chapterName}</strong>
                          <div className="muted">{recipient.fraternitySlug}</div>
                        </td>
                        <td>
                          {recipient.universityName ?? "Unknown"}
                          <div className="muted">{[recipient.city, recipient.state].filter(Boolean).join(", ") || "n/a"}</div>
                        </td>
                        <td className="monoCell" style={{ maxWidth: 260, overflowWrap: "anywhere" }}>
                          {recipient.contactValue}
                        </td>
                        <td>{recipient.status}</td>
                        <td>
                          <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap" }}>
                            {selectedSummary.channel === "instagram" ? (
                              <>
                                <a className="buttonSecondary" href={recipient.contactValue} target="_blank" rel="noreferrer">
                                  Open profile
                                </a>
                                <button type="button" className="buttonSecondary" onClick={() => void copyMessage(recipient.messageBody)}>
                                  Copy DM
                                </button>
                                <button type="button" className="buttonSecondary" onClick={() => void updateRecipientStatus(recipient.id, "sent")}>
                                  Mark sent
                                </button>
                              </>
                            ) : (
                              <button type="button" className="buttonSecondary" onClick={() => void copyMessage(`${recipient.subjectLine ?? ""}\n\n${recipient.messageBody}`)}>
                                Copy email
                              </button>
                            )}
                          </div>
                          {recipient.lastError ? <div className="muted">{recipient.lastError}</div> : null}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </article>
      </div>
    </div>
  );
}
