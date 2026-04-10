# Accuracy Conceptual Model

## Purpose
This model is the shared vocabulary for the approval-gated accuracy recovery program. It is designed to keep the five core subsystems aligned so they stop inventing conflicting meanings for chapter truth, national truth, and contact specificity.

## Shared Models

### Page Scope
| Value | Meaning |
|---|---|
| `chapter_site` | A page owned by the target chapter or clearly acting as the chapter’s home page. |
| `school_affiliation_page` | An official school-owned page about the target chapter or the school’s recognized chapter list. |
| `nationals_chapter_page` | A national-organization page that is clearly specific to the target chapter. |
| `nationals_generic` | A generic national-organization page that is not specific to the target chapter. |
| `directory_page` | A structured list/map/card page used to navigate to chapter-specific pages. |
| `unrelated` | A page that should not be used to support chapter contact truth. |

### Contact Specificity
| Value | Meaning |
|---|---|
| `chapter_specific` | The contact is clearly for the target chapter. |
| `school_specific` | The contact comes from an official school page clearly about the target chapter. |
| `national_specific_to_chapter` | The contact comes from a national page clearly about the target chapter. |
| `national_generic` | The contact is generic HQ/national contact and must not count as chapter contact. |
| `ambiguous` | The source does not provide enough specificity to trust it as chapter contact. |

### Chapter Status
| Value | Meaning |
|---|---|
| `active` | The chapter is confirmed or assumed active. |
| `inactive` | The chapter is confirmed inactive. |
| `unknown` | The system cannot yet determine whether the chapter is active. |

### Field Resolution State
| Value | Meaning |
|---|---|
| `missing` | No accepted value exists yet. |
| `resolved` | A trusted value exists. |
| `inactive` | The field is inactive because the chapter is inactive. |
| `confirmed_absent` | Authoritative evidence indicates the field does not exist. |
| `deferred` | The field is waiting on prerequisites or provider recovery. |

### Decision Outcome
| Value | Meaning |
|---|---|
| `accepted` | The system accepted the evidence and wrote or confirmed the value. |
| `rejected` | The system rejected the evidence. |
| `deferred` | The system intentionally postponed the decision. |
| `review_required` | The evidence is retained but routed to human review. |

## Shared Evidence Contract
Every subsystem should emit:

| Field | Meaning |
|---|---|
| `decision_stage` | Which stage made the decision. |
| `evidence_url` | The page that supported the decision. |
| `source_type` | Where the evidence came from. |
| `page_scope` | The conceptual page classification. |
| `contact_specificity` | Whether the evidence is chapter-specific, school-specific, national-generic, etc. |
| `confidence` | Confidence in the evidence. |
| `reason_code` | The machine-readable reason for the decision. |

## Subsystem Mapping

### 1. Source Discovery and Page Targeting
- Must classify pages before they are trusted.
- Must distinguish `nationals_generic` from `nationals_chapter_page`.
- Should prefer directory pages only as navigation surfaces, not as final supporting pages unless they are chapter-specific.

### 2. Campus / Chapter Validation
- Must resolve `active`, `inactive`, or `unknown`.
- Must prefer official school evidence.
- May mark `inactive` only when the evidence chain is strong enough to support it.

### 3. Website Resolution
- Must decide whether a page is a chapter site, school page, national chapter page, national generic page, or unrelated.
- Must support `confirmed_absent` when authoritative sources show no chapter website.

### 4. Email / Instagram Resolution
- Must only accept contacts that inherit chapter specificity from trusted evidence.
- Must reject `national_generic` contact as chapter contact.
- May accept `school_specific` and `national_specific_to_chapter` contact when the page clearly refers to the target chapter.

### 5. Queue Orchestration / Reporting
- Must expose the exact stage and reason a row was accepted, deferred, rejected, or routed to review.
- Must surface the investor-safe KPIs:
  - `complete_row`
  - `chapter_specific_contact_row`
  - `nationals_only_contact_row`
  - `inactive_validated_row`
  - `confirmed_absent_website_row`

## Success Metrics
- `complete_row`: an active, correctly identified chapter with at least one accurate `instagram` or `email` supported by chapter-specific evidence.
- `chapter_specific_contact_row`: a row with email and/or Instagram supported by `chapter_specific`, `school_specific`, or `national_specific_to_chapter` evidence.
- `nationals_only_contact_row`: a row whose present contact data is only supported by `national_generic` evidence.
- `inactive_validated_row`: a row confirmed inactive through school/activity validation evidence.
- `confirmed_absent_website_row`: a row whose website field is intentionally resolved as absent, not just missing.
