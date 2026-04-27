# Fraternity Chapter Verification Technical Model

Date: 2026-04-19
System version: 3.0.3
Primary scope: national-source discovery, chapter extraction, chapter existence verification, and active/inactive status determination

## Why this document exists

The current chapter-verification pipeline is precision-oriented, but it is not yet a `99% accurate` system across all fraternities and all chapters. The codebase already contains strong safety gates, but the historical logs also show meaningful failure modes:

- wrong national directory sources can be selected
- wrong chapter websites can be propagated from national pages
- school-office contacts can leak through under some historical runs
- some chapters are left unresolved even after large search fanout

This document is therefore intentionally dual-purpose:

1. it describes the current operating model in exact technical detail
2. it shows, with real traces, where the model succeeds and where it still fails

If the product target is truly `99% accurate chapter verification`, this document should be treated as the baseline technical map of the current system, not as proof that the system already meets that target.

## High-Level Conceptual Model

At a high level, the crawler is trying to answer four separate questions, in order:

1. `What is the safest national source for this fraternity?`
2. `What chapter records can be extracted from that national source?`
3. `Does each extracted chapter identity describe a real fraternity chapter at a real school?`
4. `Can the system safely verify the chapter as active or inactive, and can it safely attach chapter-specific contact data?`

The current model is not one monolithic classifier. It is a staged evidence pipeline:

1. `Discovery`
   - find a national directory or locator source for the fraternity
2. `Navigation and extraction`
   - inspect the national page structure
   - choose an extraction strategy
   - emit `ChapterStub` records
3. `Normalization and identity validation`
   - decide whether each stub is a valid chapter, a repairable partial identity, or an invalid non-chapter entity
4. `Field-job enrichment`
   - run targeted jobs such as `verify_school_match`, `find_website`, `find_instagram`, and `find_email`
5. `Activity validation`
   - use official school evidence to determine `confirmed_active`, `confirmed_inactive`, or `unknown`
6. `Acceptance and write gating`
   - only write values if page scope and contact specificity are safe enough
   - otherwise defer, requeue, or review

The system is designed to prefer `false negatives over false positives`. That is the right architectural instinct for chapter data, but in practice it also means the queue can accumulate unresolved work when upstream source quality is weak.

## Core Files and Responsibilities

### Discovery and source selection

- `services/crawler/src/fratfinder_crawler/discovery.py`

This file chooses the national source URL for a fraternity. It compares:

- verified registry entries
- existing configured sources already in the database
- fresh search candidates
- curated source hints

### National-page analysis and navigation

- `services/crawler/src/fratfinder_crawler/analysis/page_analyzer.py`
- `services/crawler/src/fratfinder_crawler/analysis/source_classifier.py`
- `services/crawler/src/fratfinder_crawler/analysis/chapter_link_scoring.py`
- `services/crawler/src/fratfinder_crawler/orchestration/navigation.py`
- `services/crawler/src/fratfinder_crawler/adapters/directory_v1.py`

These files decide how to parse national pages and which links are chapter-like enough to follow.

### Chapter validity and normalization

- `services/crawler/src/fratfinder_crawler/normalization/normalizer.py`
- `services/crawler/src/fratfinder_crawler/models.py`

These files decide whether an extracted record is:

- `canonical_valid`
- `repairable_candidate`
- `provisional_candidate`
- `invalid_non_chapter`

### Chapter verification and enrichment

- `services/crawler/src/fratfinder_crawler/field_jobs.py`
- `services/crawler/src/fratfinder_crawler/precision_tools.py`

These files decide:

- whether a school page proves activity
- whether a website is chapter-safe
- whether an email or Instagram is chapter-specific
- whether a candidate should be written, reviewed, deferred, or rejected

### Request orchestration

- `services/crawler/src/fratfinder_crawler/orchestration/request_graph.py`

This file coordinates end-to-end crawl execution and source recovery when the chosen national source looks weak.

## Data Model and Important Field Names

The verification engine uses a typed model with explicit scopes, specificity labels, and queue states. The most important constants live in `models.py`.

```python
PAGE_SCOPE_CHAPTER_SITE = "chapter_site"
PAGE_SCOPE_SCHOOL_AFFILIATION = "school_affiliation_page"
PAGE_SCOPE_NATIONALS_CHAPTER = "nationals_chapter_page"
PAGE_SCOPE_NATIONALS_GENERIC = "nationals_generic"
PAGE_SCOPE_DIRECTORY = "directory_page"
PAGE_SCOPE_UNRELATED = "unrelated"

CONTACT_SPECIFICITY_CHAPTER = "chapter_specific"
CONTACT_SPECIFICITY_SCHOOL = "school_specific"
CONTACT_SPECIFICITY_NATIONAL_CHAPTER = "national_specific_to_chapter"
CONTACT_SPECIFICITY_NATIONAL_GENERIC = "national_generic"
CONTACT_SPECIFICITY_AMBIGUOUS = "ambiguous"

CHAPTER_STATUS_ACTIVE = "active"
CHAPTER_STATUS_INACTIVE = "inactive"
CHAPTER_STATUS_UNKNOWN = "unknown"
```

These constants are central to the acceptance model:

- a `chapter_site` page is the safest contact source
- a `school_affiliation_page` can validate chapter existence and sometimes safely provide chapter contact
- a `nationals_chapter_page` can count if it is chapter-specific
- a `nationals_generic` page must not count as chapter contact

Important typed records:

### `ChapterStub`

Emitted from national-page parsing.

Fields:

- `chapter_name`
- `university_name`
- `detail_url`
- `outbound_chapter_url_candidate`
- `confidence`
- `provenance`

### `ChapterTarget`

Represents a link destination being considered for follow-up.

Fields:

- `url`
- `target_type`
- `source_class`
- `follow_allowed`
- `rejection_reason`
- `host`

### `ChapterIdentity`

Represents how complete the identity is before normalization.

Fields:

- `chapter_name`
- `university_name`
- `source_class`
- `chapter_intent_signals`
- `identity_complete`

### `ChapterValidityDecision`

The normalized judgment for a candidate chapter.

Fields:

- `validity_class`
- `invalid_reason`
- `repair_reason`
- `next_action`
- `semantic_signals`

### `FieldJob`

Represents enrichment and verification work for one chapter field.

Fields:

- `field_name`
- `payload`
- `attempts`
- `max_attempts`
- `website_url`
- `instagram_url`
- `contact_email`
- `university_name`
- `chapter_status`
- `field_states`
- `queue_state`
- `validity_class`
- `repair_state`
- `blocked_reason`
- `terminal_outcome`

### Important `FieldJob.payload` keys

The `payload` object is part of the effective runtime schema. The most important keys consumed by the engine are:

- `contactResolution.supportingPageUrl`
- `contactResolution.supportingPageScope`
- `contactResolution.pageScope`
- `contactResolution.reasonCode`
- `provider_attempts`
- `schoolValidationStatus`
- `terminal_no_signal_count`
- `transient_provider_failures`

These keys are how the system carries context across attempts. They tell later retries:

- which supporting page was already accepted
- which page scope was assigned to that page
- which providers have already been tried
- whether the job has become low-signal or network-blocked

### Important `completed_payload` keys

When a field job finishes, the engine generally emits:

- `status`
- `field`
- `sourceUrl`
- `reasonCode`
- `evidenceUrl`
- `resolutionEvidence`
- `decision_trace`

Important nested `resolutionEvidence` fields include:

- `pageScope`
- `contactSpecificity`
- `decisionStage`
- `reasonCode`

These are the fields that make the later acceptance model explainable.

### Important request-progress fields

The request graph persists discovery state under structured keys:

- `progress.discovery.sourceUrl`
- `progress.discovery.sourceConfidence`
- `progress.discovery.confidenceTier`
- `progress.discovery.sourceProvenance`
- `progress.discovery.fallbackReason`
- `progress.discovery.sourceQuality`
- `progress.discovery.selectedCandidateRationale`
- `progress.discovery.resolutionTrace`
- `progress.discovery.candidates`

### `ActivityValidationDecision`

The activity-validation record returned by official school validation.

Fields:

- `school_policy_status`
- `chapter_activity_status`
- `evidence_url`
- `evidence_source_type`
- `reason_code`
- `source_snippet`
- `confidence`
- `metadata`

### `AuthoritativeBundle`

The bundle of chapter-safe signals extracted from authoritative sources.

Fields:

- `website_match`
- `email_match`
- `instagram_match`
- `website_confirmed_absent`
- `authoritative_context_found`
- `evidence_url`
- `evidence_source_type`
- `reason_code`

## End-to-End Architecture

### Stage 1: Discover the fraternity national source

The source selector is implemented in `discover_source(...)` in `discovery.py`.

The algorithm is:

1. normalize the fraternity identity
2. try the verified registry
3. try existing configured sources
4. if neither is safe enough, run search queries
5. score each candidate
6. reject weak, blocked, or conflicting candidates
7. apply same-host directory recovery or curated hints if needed
8. apply a final source-quality gate

The discovery result is returned as a `DiscoveryResult`:

```python
@dataclass(slots=True)
class DiscoveryResult:
    fraternity_name: str
    fraternity_slug: str
    selected_url: str | None
    selected_confidence: float
    confidence_tier: str
    candidates: list[DiscoveryCandidate]
    source_provenance: str | None
    fallback_reason: str | None
    source_quality: DiscoverySourceQuality | None
    selected_candidate_rationale: str | None
    resolution_trace: list[dict[str, Any]]
```

That `resolution_trace` field is critical. It records each decision step and is the main audit trail for source selection.

### Stage 2: Score national-source candidates

Discovery scoring is heuristic and guardrail-heavy. The key function is `_score_candidate(...)`.

```python
def _score_candidate(fraternity_name: str, fraternity_slug: str, result: SearchResult) -> float:
    score = 0.36
    ...
    if _contains_phrase(lowered_title, fraternity_name):
        score += 0.2
    if trusted_host_hints and _host_matches_any_hint(host, trusted_host_hints):
        score += 0.35
    if any(marker in path for marker in _DIRECTORY_MARKERS):
        score += 0.12
    if any(marker in combined for marker in ("alumni chapter", "alumni association", "alumni")):
        score -= 0.3
    ...
    identity_guard = tool_source_identity_guard(...)
    if identity_guard.decision == "match":
        score += 0.12
    elif identity_guard.decision == "weak_match":
        score += 0.02
    else:
        score = min(score, 0.25)
    if "cross_fraternity_conflict" in identity_guard.reason_codes:
        score = 0.0
```

What this means in practice:

- official-looking directory paths are rewarded
- trusted fraternity host hints are strongly rewarded
- alumni and non-organization contexts are penalized
- the `tool_source_identity_guard(...)` can hard-cap or fully reject a candidate

This is why discovery is not simply taking the top web result. It is trying to distinguish:

- actual national directory pages
- chapter-owned sites
- alumni sites
- unrelated entities
- generic marketing pages

### Stage 3: Evaluate source quality independently of search score

Search score and source quality are not the same thing.

`_source_quality_from_url(...)` computes a separate structural quality score from the URL itself:

```python
def _source_quality_from_url(url: str | None) -> DiscoverySourceQuality:
    ...
    score = 0.56
    if parsed.scheme not in {"http", "https"}:
        score -= 0.4
    if is_blocked:
        score -= 0.72
    if any(marker in path for marker in _DIRECTORY_MARKERS):
        score += 0.24
    if path.strip("/") == "":
        score -= 0.08
    elif len(path_segments) == 1 and path_segments[0] in _GENERIC_INFO_PATH_SEGMENTS:
        score -= 0.16
```

This matters because a page can be search-relevant but structurally bad as a crawl source. For example:

- root homepages
- member portals
- generic about pages
- blocked hosts like Wikipedia

### Stage 4: Analyze the selected national page

Once a source is chosen, the page is analyzed structurally.

`analyze_page(...)` extracts:

- headings
- table count
- repeated block count
- link count
- JSON-LD presence
- inline script JSON presence
- map widget presence
- pagination presence
- `probable_page_role`
- `text_sample`

This is then classified by `classify_source(...)` into one of:

- `script_embedded_data`
- `locator_map`
- `static_directory`
- `unsupported_or_unclear`

This is the bridge between "we found a page" and "we know how to parse it".

### Stage 5: Detect national navigation mode

`detect_chapter_index_mode(...)` in `navigation.py` decides how chapter listings are likely represented:

- `member_portal_gated`
- `map_or_api_locator`
- `direct_chapter_list`
- `internal_detail_pages`
- `mixed`

The decision combines:

- page analysis
- source classification
- embedded-data hints
- optional source-metadata overrides

This mode is used to select safe extraction and follow strategies.

### Stage 6: Extract chapter stubs

Chapter extraction is multi-strategy, not single-parser.

The system currently uses:

- `locator_api`
- `script_json`
- `repeated_block`
- `table`
- anchor extraction
- map-config extraction
- Wix-specific extraction
- Elementor-specific extraction
- plain-text "chapter roll" extraction

From `extract_chapter_stubs(...)`:

```python
strategies: list[str] = configured_stub_strategies or ["repeated_block", "table"]
if embedded_data.found and embedded_data.api_url:
    strategies.insert(0, "locator_api")
if embedded_data.found and embedded_data.data_type in {"json_ld", "script_json"}:
    strategies.insert(0, "script_json")
...
stubs.extend(_extract_anchor_stubs(html, source_url))
stubs.extend(_extract_map_config_state_stubs(html, source_url))
stubs.extend(_extract_wix_chapter_link_stubs(html, source_url))
stubs.extend(_extract_elementor_chaptername_stubs(html, source_url))
stubs.extend(_extract_chapter_roll_text_stubs(html, source_url))
```

The purpose is to handle different directory shapes without depending on a single DOM pattern.

### Stage 7: Decide which extracted links may be followed

The system does not blindly follow all links.

`classify_chapter_target(...)` labels a candidate URL as:

- `national_listing`
- `national_detail`
- `institutional_page`
- `chapter_owned_site`
- `social_page`
- `unknown`

The follow policy is restrictive:

- same-host national detail pages are followable
- many `.edu` pages are followable
- chapter-owned wider-web sites are often not followed in main crawl
- social pages are not followed as chapter-detail pages

Important rule:

```python
if lowered.startswith("http://") or lowered.startswith("https://"):
    return ChapterTarget(
        url=normalized_url,
        target_type="chapter_owned_site",
        source_class="wider_web",
        follow_allowed=False,
        rejection_reason="chapter_site_only",
        host=host,
    )
```

This is a precision decision. The national-page crawl prefers to store wider-web chapter-site hints rather than deeply crawl them during the extraction stage.

### Stage 8: Convert stubs into typed chapter candidates

`build_chapter_candidates(...)` wraps each stub into:

- `ChapterIdentity`
- `ChapterTarget[]`
- `ChapterValidityDecision`

The validity decision comes from `classify_chapter_validity(...)` in `normalizer.py`.

The logic is straightforward:

- if the entity is semantically invalid, quarantine it
- if it has both chapter and institution signals, mark it `canonical_valid`
- if it has only chapter-like signals, mark it `repairable_candidate`
- if it comes from broader web and is incomplete, quarantine or keep provisional only

This step is how the system tries to prevent bogus rows like:

- departments
- academic programs
- years
- percentages
- people names
- generic university units

That invalid-entity quarantine is one of the strongest safety mechanisms in the platform.

## How Chapter Existence and Status Are Verified

The current system does not treat "found on the internet" as proof that a chapter exists. It tries to verify chapter existence from more authoritative evidence.

### Primary verification inputs

The system prefers these evidence classes:

1. official school pages
2. accepted chapter websites
3. accepted chapter-specific national pages
4. already accepted supporting pages in provenance

These are treated as more reliable than raw search results.

### Official school status validator

The central active/inactive decision tool is `tool_school_chapter_list_validator(...)` in `precision_tools.py`.

It uses:

- `school_name`
- `fraternity_name`
- `fraternity_slug`
- `page_url`
- `title`
- `text`
- `html`

It extracts:

- anchor texts
- list items
- headings
- tabbed chapter sections
- fraternity-section text
- suspended-section text
- closed-section text
- official-page signals
- historical/article context signals

The active/inactive decision core is:

```python
if official_list_page and active_roster_signal and fraternity_match:
    return PrecisionDecision(
        decision="confirmed_active",
        confidence=0.93,
        reason_codes=["fraternity_present_on_official_school_list"],
    )

if conclusive_roster_page and org_anchor_count >= 3 and roster_excludes_target:
    return PrecisionDecision(
        decision="confirmed_inactive",
        confidence=0.9,
        reason_codes=["fraternity_absent_from_official_school_list"],
    )
```

This means:

- `confirmed_active` requires an official school list context plus a positive fraternity match
- `confirmed_inactive` requires a conclusive roster page that appears complete enough to make absence meaningful
- otherwise the result is `unknown`

### Why `unknown` exists

The validator returns `unknown` when:

- the page is not official enough
- the page is historical rather than current
- the page looks like an article rather than a roster
- the school identity is weak
- the page is official but inconclusive

This is the right design for accuracy. A system targeting `99% correctness` should refuse to infer inactivity from weak evidence.

### Activity gate in field jobs

Field jobs call `_resolve_activity_gate(...)` before writing contact fields.

The logic is:

1. resolve school policy
2. if the school bans Greek life, mark the chapter inactive
3. resolve chapter activity from official school evidence
4. if `confirmed_inactive`, mark the chapter inactive
5. if `confirmed_active`, continue
6. if `unknown`, do not use activity status as proof either way

Relevant code:

```python
def _resolve_activity_gate(self, job: FieldJob, *, target_field: str) -> FieldJobResult | None:
    school_policy = self._get_or_resolve_school_policy(job)
    if school_policy.school_policy_status == "banned":
        return self._mark_chapter_inactive(job, target_field=target_field, decision=school_policy)

    chapter_activity = self._get_or_resolve_chapter_activity(job)
    if chapter_activity.chapter_activity_status == "confirmed_inactive":
        return self._mark_chapter_inactive(job, target_field=target_field, decision=chapter_activity)
    if chapter_activity.chapter_activity_status == "confirmed_active":
        self._trace("chapter_activity_validation", status="confirmed_active", school=self._school_name_for_job(job))
    return None
```

### How inactive chapters are written

When a chapter is proved inactive, `_mark_chapter_inactive(...)`:

- writes `chapter_status = inactive`
- completes sibling field jobs for the chapter
- stores:
  - `reasonCode`
  - `evidenceUrl`
  - `resolutionEvidence`
  - `decision_trace`

That means inactivity is treated as a first-class terminal state, not as a missing-data condition.

## How Navigation and Fallbacks Work During Enrichment

Once chapter candidates exist, the system creates field jobs such as:

- `verify_school_match`
- `verify_website`
- `find_website`
- `find_instagram`
- `find_email`

### Supporting-page readiness gate

Some jobs are not allowed to run until a supporting page exists.

`_job_supporting_page_ready(...)` returns true if the job already has one of:

- a confident website
- a `supportingPageUrl` with scope:
  - `chapter_site`
  - `school_affiliation_page`
  - `nationals_chapter_page`
- a `confirmed_absent` website state plus another accepted supporting page

This matters most for email. The current logic intentionally blocks `find_email` when there is no trusted page context.

### Deterministic action selection

The deterministic enrichment controller prefers existing evidence over fresh search.

```python
if bool(context.get("supporting_page_present")):
    return "parse_supporting_page"
if job.field_name == FIELD_JOB_VERIFY_SCHOOL:
    return "verify_school"
if job.field_name in {FIELD_JOB_FIND_WEBSITE, FIELD_JOB_VERIFY_WEBSITE}:
    return "verify_website" if ... else "search_web"
if job.field_name == FIELD_JOB_FIND_INSTAGRAM:
    return "search_social"
if job.field_name == FIELD_JOB_FIND_EMAIL and bool(context.get("website_prerequisite_unmet")):
    return "defer"
return "search_web"
```

That is the conceptual policy:

- parse what we already have first
- only search when prerequisites are satisfied
- defer rather than guess

### Authoritative bundle

Before raw search results are accepted, the engine tries to build an `AuthoritativeBundle`.

The bundle is assembled from official-school validation documents whose scope classifier says they are one of:

- `school_affiliation`
- `nationals`
- `chapter_site`

The bundle can yield:

- `website_match`
- `email_match`
- `instagram_match`
- `website_confirmed_absent`

This is one of the most important behaviors in the codebase because it allows the system to:

- resolve contact fields from already trusted sources
- reject generic national contacts
- mark a website as `confirmed_absent` when strong evidence exists but no chapter site is found

### Rejection of generic nationals contacts

The code explicitly blocks generic national contacts from counting as chapter data.

```python
if field_key == "website_url" and evidence.get("pageScope") == PAGE_SCOPE_NATIONALS_GENERIC:
    continue
if field_key in {"contact_email", "instagram_url"} and evidence.get("contactSpecificity") == CONTACT_SPECIFICITY_NATIONAL_GENERIC:
    continue
```

The same rule is repeated in search-candidate acceptance.

This is the guard that should prevent rows like:

- `@thetachiihq`
- `ihq@thetachi.org`

from counting as chapter-specific contact for individual chapters.

### Search fallback and retry reasons

When no candidate is found, `_no_candidate_error(...)` determines whether the job is:

- `provider_degraded`
- `transient_network`
- `provider_low_signal`
- effectively terminal no-candidate

Relevant logic:

```python
if self._search_skipped_due_to_degraded_mode:
    return RetryableJobError(..., reason_code="provider_degraded")
if self._search_errors_encountered:
    ...
    return RetryableJobError(..., reason_code="transient_network")
...
return RetryableJobError(message, backoff_seconds=self._base_backoff_seconds, reason_code="provider_low_signal")
```

That means unresolved chapters do not all mean the same thing. Some are:

- blocked by provider health
- blocked by missing prerequisites
- blocked by lack of safe evidence
- blocked because the identity itself is weak

## How Sources Are Scored

There are four separate scoring layers in the current system.

### 1. National-source discovery score

Computed by `_score_candidate(...)` in `discovery.py`.

Signals:

- fraternity phrase in title
- fraternity phrase in snippet
- fraternity token overlap with host
- trusted host hints
- directory/path markers
- official/international/fraternity phrases
- context hits for chapter/directory/fraternity
- penalties for alumni, travel, PDFs, non-org context
- identity guard outcome

### 2. Source-quality URL score

Computed by `_source_quality_from_url(...)`.

Signals:

- HTTP/HTTPS scheme
- blocked host
- directory path
- weak path markers
- generic root path
- generic info path

### 3. Chapter-link score

Computed by `score_chapter_link(...)`.

Signals:

- positive labels like `chapter website`, `chapter directory`, `find a chapter`
- positive URL markers like `chapters`, `chapter-directory`
- context containing `chapter`, `university`, or `college`
- penalties for news/blog/resource/staff/social links

### 4. Candidate write thresholds

Computed inside `field_jobs.py`.

Search-derived values are held to stricter standards than values discovered on already trusted pages.

Examples:

- search-derived websites can require `0.96` or stronger confidence
- search-derived email needs about `0.92`
- search-derived Instagram needs about `0.90`

This thresholding is why many jobs complete with no updates, review items, or requeues instead of silent writes.

## How Status Is Determined From Page Content

The actual active/inactive model is text- and structure-driven, not a simple keyword match.

### Signals that support `active`

- page is official school domain
- page looks like a chapter list / roster / scorecard / Greek-life registry
- fraternity appears in the roster or the active fraternity section
- chapter roster structure is strong enough to imply current status

Examples of supporting cues:

- `recognized chapters`
- `chapters at`
- `chapter scorecards`
- `active chapters`
- tabbed sections with `fraternities`, `suspended`, `closed`

### Signals that support `inactive`

Inactive is only inferred when the absence is meaningful.

Required pattern:

- page is an official, conclusive roster page
- organization-anchor density is high enough
- the target fraternity is absent from active, suspended, and closed sections checked by the validator

This is much stricter than simple non-appearance.

### Signals that force `unknown`

- article/news context
- historical context
- non-official page
- missing school identity
- official page not conclusive

This conservative design is necessary if the system is expected to be accurate at investor-demo or production scale.

## Current High-Risk Failure Modes

The historical logs show that the intended safety model and the actual runtime outcomes are not always aligned.

### Failure mode 1: wrong national source or wrong optimized source

Example: `Pi Kappa Alpha` had a top search candidate of `https://www.transypike.com/faq`, which is a chapter site, not the national directory. The system later optimized away from it, but only after initially selecting it.

### Failure mode 2: wrong chapter website propagated across multiple chapters

Example: multiple `Delta Chi` chapters were assigned `http://www.deltachi.ca/`, which appears to be the same unrelated site reused across different schools.

### Failure mode 3: school-office contact leakage

Example: a historical `find_email` log for Denver completed with `studentengagement@du.edu`, which is clearly not chapter-specific.

### Failure mode 4: large search fanout with no safe write

Example: Denver targeted website validation executed 15 queries, rejected 70 candidates, and still ended in `terminal_no_candidate`.

These examples show that the platform has meaningful guardrails, but it also still has real precision gaps and resolution inefficiencies.

## What the Request Graph Stores

When source recovery succeeds, the request graph writes discovery metadata into request progress:

- `sourceUrl`
- `sourceConfidence`
- `confidenceTier`
- `sourceProvenance`
- `fallbackReason`
- `sourceQuality`
- `selectedCandidateRationale`
- `resolutionTrace`
- `candidates`

This matters because the source-recovery layer is the first place to inspect when the chapter crawl looks implausible or low-yield.

## Ten Real Examples From Logs and Monitor Snapshots

The examples below are based on actual runtime artifacts in:

- `docs/SystemReport/overnight_demo_runs_2026-04-09/monitor/*.json`
- `logs/ambiguous_school_validation_2026-04-01_targeted.log`
- `logs/20260401_005611_dchi_website_random.log`
- `logs/20260401_002454_delta_email.log`

Each example shows the actual data flow and parameters that drove the decision.

### Example 1: `Chi Psi` source selected from verified registry

Artifact:

- `docs/SystemReport/overnight_demo_runs_2026-04-09/monitor/snapshot_0036.json`

Inputs:

- `fraternity_name = "Chi Psi"`
- normalized slug: `chi-psi`

Observed flow:

1. `identity_normalization`
2. `checked_verified_registry`
   - `found = true`
   - `confidence = 0.85`
   - `http_status = 200`
   - `national_url = https://chipsi.org/where-we-are/`
3. `checked_existing_sources`
   - existing configured source matched the same URL
4. `selected_verified_registry_candidate`

Observed output:

- `sourceUrl = https://chipsi.org/where-we-are/`
- `sourceProvenance = verified_registry`
- `selectedCandidateRationale = null`

Interpretation:

This is the cleanest path. Verified registry and existing source agreed, and the verified registry won because it was active and healthy.

### Example 2: `Theta Xi` source selected from existing configured source

Artifact:

- `docs/SystemReport/overnight_demo_runs_2026-04-09/monitor/snapshot_0036.json`

Inputs:

- `fraternity_name = "Theta Xi"`
- normalized slug: `theta-xi`

Observed flow:

1. `checked_verified_registry`
   - `found = false`
2. `checked_existing_sources`
   - `list_url = https://www.thetaxi.org/chapters-and-colonies/`
   - `parser_key = directory_v1`
   - `last_run_status = succeeded`
   - `confidence = 0.9`
3. `selected_existing_source_candidate`

Observed output:

- `sourceUrl = https://www.thetaxi.org/chapters-and-colonies/`
- `sourceProvenance = existing_source`

Interpretation:

This is the intended fallback when a registry row does not exist but an already-validated configured source does.

### Example 3: `Sigma Alpha Epsilon` rejected blocked existing source and moved to search

Artifact:

- `docs/SystemReport/overnight_demo_runs_2026-04-09/monitor/snapshot_0036.json`

Inputs:

- `fraternity_name = "Sigma Alpha Epsilon"`
- existing source URL:
  - `https://en.wikipedia.org/wiki/List_of_Sigma_Alpha_Epsilon_chapters`

Observed flow:

1. `checked_verified_registry`
   - `found = false`
2. `checked_existing_sources`
   - `candidate_valid = false`
   - `candidate_reasons = ["blocked_host"]`
3. `rejected_existing_source_candidate`
4. `source_identity_guard`
   - selected `https://sae.net/chapter-resources/`
5. `selected_search_candidate`

Observed output:

- `sourceUrl = https://sae.net/chapter-resources/`
- `sourceProvenance = search`
- `fallbackReason = existing_source_invalid`

Interpretation:

This shows the blocking of Wikipedia-style sources working as designed.

### Example 4: `Pi Kappa Alpha` had a high-scoring wrong search winner before later optimization

Artifact:

- `docs/SystemReport/overnight_demo_runs_2026-04-09/monitor/snapshot_0036.json`

Inputs:

- `fraternity_name = "Pi Kappa Alpha"`
- no verified registry row
- existing source was blocked Wikipedia

Observed flow:

1. `checked_existing_sources`
   - blocked Wikipedia candidate rejected
2. search candidate list included:
   - `https://www.transypike.com/faq`
   - `https://pikes.org/membership/join/find-a-chapter/`
   - `https://www.wm.edu/offices/fsl/about/chapters/fraternities/pka.php`
3. `selected_search_candidate`
   - initial winner: `https://www.transypike.com/faq`
4. `optimized_source_selection`
   - `previousUrl = https://www.transypike.com/faq`
   - `nextUrl = https://www.wm.edu/offices/fsl/about/chapters/fraternities/pka.php`

Observed output:

- request-level `sourceUrl = https://pikes.org/membership/join/find-a-chapter/`
- discovery trace shows an intermediate path where a chapter site was initially treated as a strong match
- the trace and the final stored request URL are not perfectly aligned, which is itself an audit signal

Interpretation:

This is a critical failure pattern. The search scorer can overvalue strong fraternity language on a chapter-owned or school-specific page unless host and directory logic pull it back.

### Example 5: `Delta Chi` Rutgers website job used school-first queries, a nationals entry, and still produced no write

Artifact:

- `logs/ambiguous_school_validation_2026-04-01_targeted.log`

Parameters observed:

- `field_name = find_website`
- `chapter_slug = rutgers-chapter-rutgers`
- `source_slug = delta-chi-main`
- preflight provider: `searxng_json`

Observed query sequence:

- `"delta chi" Rutgers student organization site:.edu`
- `"delta chi" Rutgers greek life site:.edu`
- `"delta chi" Rutgers ifc site:.edu`
- `"Rutgers" fraternities site:.edu`
- `"delta chi" Rutgers Chapter Rutgers chapter website`
- `"delta chi" Rutgers chapter website`
- `"delta chi" Rutgers official chapter site`
- `"delta chi" Rutgers`
- `deltachi Rutgers chapter website`

Observed event flow:

1. `field_job_claimed`
2. repeated `search_query_executed`
3. `nationals_entries_collected`
   - `entry_count = 1`
4. `field_job_completed`
   - `updates = {}`
   - `field_states = {}`

Interpretation:

This is a no-write success path. The job ran, but the system still found nothing safe enough to persist.

### Example 6: `Delta Chi` Denver website job rejected 70 candidates and ended in terminal no-candidate

Artifact:

- `logs/ambiguous_school_validation_2026-04-01_targeted.log`

Parameters observed:

- `field_name = find_website`
- `chapter_slug = denver-chapter-denver`

Observed rejection summary:

- `search_queries_attempted = 15`
- `search_queries_succeeded = 15`
- `top_reasons`:
  - `website:document_not_relevant` count `56`
  - `website:ambiguous_school_tier1_generic` count `7`
  - `website:low_specificity_tier1` count `4`
  - `website:document_asset` count `2`
  - `website:low_signal_url` count `1`

Observed outcome:

- `field_job_requeued`
- `retry_reason = terminal_no_candidate`

Interpretation:

This shows the precision filters working, but also shows why throughput can collapse: the system spent a large search budget to conclude "nothing safe".

### Example 7: `Delta Chi` Jacksonville State website job wrote the same suspicious URL found from nationals entries

Artifact:

- `logs/20260401_005611_dchi_website_random.log`

Parameters observed:

- `field_name = find_website`
- `chapter_slug = jacksonville-state-chapter-jacksonville-state`
- `source_slug = delta-chi-main`

Observed event flow:

1. school-affiliation queries executed
2. `nationals_entries_collected`
   - `entry_count = 1`
3. `nationals_target_candidates_found`
   - `candidate_count = 1`
4. `field_job_completed`
   - `field_states = {"website_url": "found"}`
   - `updates = {"website_url": "http://www.deltachi.ca/"}`

Interpretation:

This is a likely false positive. The same external URL later appears for other unrelated chapters. This is exactly the kind of cross-chapter contamination a `99% accurate` system must eliminate.

### Example 8: `Delta Chi` Denver website job also wrote the same suspicious URL in a different run

Artifact:

- `logs/20260401_005611_dchi_website_random.log`

Parameters observed:

- `field_name = find_website`
- `chapter_slug = denver-chapter-denver`

Observed event flow:

1. school-affiliation queries executed
2. `nationals_target_candidates_found`
   - `candidate_count = 1`
3. `field_job_completed`
   - `updates = {"website_url": "http://www.deltachi.ca/"}`

Interpretation:

The same output URL being written for different schools is a strong indicator that the nationals-entry matcher accepted a generic or mismatched website.

### Example 9: `Delta Chi` LSU email job collected a nationals entry, ran five queries, and still safely refused to write

Artifact:

- `logs/20260401_002454_delta_email.log`

Parameters observed:

- `field_name = find_email`
- `chapter_slug = lsu-lsu`
- `source_slug = delta-chi-main`

Observed query sequence:

- `site:www.lsu.edu "delta chi" contact email`
- `site:www.lsu.edu "delta chi" officers email`
- `"delta chi" Lsu Lsu contact email`
- `"delta chi" Lsu Lsu email`
- `"delta chi" Lsu contact email`

Observed event flow:

1. `nationals_entries_collected`
   - `entry_count = 1`
2. multiple school-targeted email queries
3. `candidate_rejection_summary`
   - `top_reasons = [{"reason": "email:search_result_not_useful", "count": 14}]`
4. `field_job_requeued`
   - `retry_reason = terminal_no_candidate`

Interpretation:

This is an example of the system doing the safer thing. It had evidence and search coverage, but it still refused to invent an email.

### Example 10: `Delta Chi` Denver email job historically wrote a school-office email

Artifact:

- `logs/20260401_002454_delta_email.log`

Parameters observed:

- `field_name = find_email`
- `chapter_slug = denver-denver`

Observed outcome:

- `field_job_completed`
- `field_states = {"contact_email": "found"}`
- `updates = {"contact_email": "studentengagement@du.edu"}`

Interpretation:

This is a clear historical false positive. `studentengagement@du.edu` is institution-level, not chapter-specific. It demonstrates that earlier runtime behavior allowed school-office leakage despite the current intended specificity rules.

## What must be true to reach 99% accuracy

The current codebase already contains several correct architectural ideas:

- typed page scopes
- typed contact specificity
- official-school activity validation
- authoritative-first resolution
- invalid-entity quarantine
- blocked-host rejection

But the historical traces show that `99% accuracy` will require stricter enforcement in at least these areas:

1. `nationals directory ingestion`
   - same website cannot be safely reused across multiple schools without school-specific proof
2. `school-office rejection`
   - institution-level addresses and social handles must be rejected earlier and more consistently
3. `source-selection hardening`
   - chapter-owned pages and school FSL pages cannot outrank national directories during source selection
4. `status-first verification`
   - official-school validation should dominate existence and status decisions before any broader search-derived contact write
5. `chapter-specific nationals handling`
   - a national page can count only if the page is truly scoped to one chapter, not one fraternity globally

## Final Takeaway

The current architecture is a layered verification system, not a generic crawler:

- discovery chooses a national source
- structure analysis decides how to parse it
- navigation extracts chapter candidates
- normalization filters invalid entities
- field jobs verify existence, status, and contact fields
- page-scope and specificity rules decide whether data is safe to write

That is the correct conceptual model for a high-accuracy fraternity chapter platform.

However, the logs show that the current implementation is still below a `99% accurate` standard. The most important gap is not lack of search effort. It is insufficiently strict enforcement of chapter-specificity and source scope when evidence is noisy, especially on national directories and school pages.
