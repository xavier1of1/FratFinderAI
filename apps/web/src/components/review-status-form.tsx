"use client";

import { useState } from "react";

const statusOptions = ["open", "triaged", "resolved", "ignored"] as const;

type ReviewStatus = (typeof statusOptions)[number];
const allowedTransitions: Record<ReviewStatus, ReviewStatus[]> = {
  open: ["open", "triaged", "ignored"],
  triaged: ["triaged", "resolved", "ignored"],
  resolved: ["resolved"],
  ignored: ["ignored"]
};

export function ReviewStatusForm({ id, currentStatus }: { id: string; currentStatus: ReviewStatus }) {
  const [status, setStatus] = useState<ReviewStatus>(currentStatus);
  const [actor, setActor] = useState("local-operator");
  const [notes, setNotes] = useState("");
  const [isSaving, setIsSaving] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const onSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsSaving(true);
    setErrorMessage(null);

    try {
      const response = await fetch(`/api/review-items/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status, actor, notes, triageNotes: notes, resolvedBy: actor })
      });

      const payload = (await response.json()) as
        | { success: true }
        | { success: false; error: { message: string; code: string } };

      if (!response.ok || !payload.success) {
        const message = payload.success ? `Request failed with ${response.status}` : payload.error.message;
        throw new Error(message);
      }

      window.location.reload();
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : "Unexpected error");
    } finally {
      setIsSaving(false);
    }
  };

  return (
    <form onSubmit={onSubmit} style={{ display: "grid", gap: "0.3rem", minWidth: "220px" }}>
      <select value={status} onChange={(event) => setStatus(event.target.value as ReviewStatus)}>
        {statusOptions.filter((option) => allowedTransitions[currentStatus].includes(option)).map((option) => (
          <option key={option} value={option}>
            {option}
          </option>
        ))}
      </select>
      <input
        value={actor}
        onChange={(event) => setActor(event.target.value)}
        placeholder="actor"
        aria-label="actor"
      />
      <input
        value={notes}
        onChange={(event) => setNotes(event.target.value)}
        placeholder="notes"
        aria-label="notes"
      />
      <button disabled={isSaving} type="submit">
        {isSaving ? "Saving" : "Update"}
      </button>
      {errorMessage ? <span className="muted">{errorMessage}</span> : null}
    </form>
  );
}
