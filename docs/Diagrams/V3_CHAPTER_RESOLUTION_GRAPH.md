# V3 Chapter Resolution Graph

This subgraph resolves one chapter entity from partial evidence into a confidence-scored canonical update, review item, or deferred follow-up.

It makes the write policy explicit:

- high-confidence values can auto-write
- medium-confidence values persist as evidence and route to review
- blocked or degraded cases queue deferred work

## Chapter Resolution Subgraph

```mermaid
flowchart TD
  C0["chapter_input<br/>stub or partial chapter entity"] -->
  C1["load_existing_context<br/>existing chapter row, provenance, evidence, field states"]

  C1 --> C2["resolve_primary_site_signal<br/>detail page or direct website candidate"]
  C2 --> C3["verify_website_candidate<br/>host affinity, chapter fit, school fit"]
  C3 -->|verified| C4["write_or_stage_website"]
  C3 -->|weak or conflict| C5["persist_website_evidence_only"]

  C4 --> C6["search_contact_email<br/>website confidence gate satisfied"]
  C5 --> C6
  C6 --> C7["verify_email_candidate<br/>domain trust, role relevance, conflict checks"]
  C7 -->|high confidence| C8["write_or_stage_email"]
  C7 -->|medium or conflict| C9["persist_email_evidence_only"]

  C8 --> C10["search_instagram"]
  C9 --> C10
  C10 --> C11["verify_social_candidate<br/>handle fit, school markers, host validity"]
  C11 -->|high confidence| C12["write_or_stage_instagram"]
  C11 -->|medium or conflict| C13["persist_social_evidence_only"]

  C12 --> C14["final_confidence_gate<br/>decide auto-write versus review versus defer"]
  C13 --> C14
  C14 -->|safe| C15["commit_canonical_updates"]
  C14 -->|needs human| C16["create_review_item"]
  C14 -->|blocked or degraded| C17["queue_followup_job"]

  C15 --> C18["emit_provenance_and_summary"]
  C16 --> C18
  C17 --> C18
```

## Decision Rules

- Website is the first trust anchor for later email and social enrichment.
- Email search should normally wait until a trusted website exists, except under explicit escape-hatch policy after repeated provider-block outcomes.
- Canonical writes should never silently replace a previously trusted conflicting value.
- Evidence should still be persisted even when a value is not yet safe to write.

## Output Modes

- `canonical update`: high-confidence, policy-safe value can be written immediately.
- `evidence only`: useful candidate persists in `chapter_evidence` but does not mutate canonical fields.
- `review`: operator intervention required because trust or conflict rules are unresolved.
- `defer`: follow-up job should continue resolution later because budgets, dependencies, or provider health do not permit safe completion now.
