# Phase 5 Contact Approval Report

## Goal
- Targeted problem: legacy `contact_email` and `instagram_url` values still contained chapter-unsafe data, especially nationals HQ contact, wrong-school school-office contact, and unsupported chapter guesses.
- Phase: bounded Phase 5 chapter-specific email and Instagram cleanup.
- Scope: legacy contact reconciliation only. This phase did not widen throughput or start new enrichment.

## Generalized Solution
- Reusable logic added:
  - reconcile legacy contact values against the current conceptual model instead of trusting the raw column value
  - backfill trusted provenance when a value is supported by a `chapter_site`, `school_affiliation_page`, or `nationals_chapter_page`
  - clear unsupported values when the supporting page is unrelated, generic-national, or a generic school page without local chapter identity
  - leave ambiguous values in a review bucket instead of over-accepting them
- Mid-phase generalized fixes that were required:
  - two-letter fraternity initials no longer count as sufficient email/page identity
  - fraternity-branded standalone hosts are no longer mislabeled as official school-affiliation pages
  - generic school-affiliation pages without page-local chapter identity now clear bad emails/handles instead of surviving as live data
  - direct Instagram evidence now preserves the original Instagram URL instead of storing the login redirect as the supporting page
- Why this is generalized rather than fraternity-specific:
  - all decisions are based on page scope, supporting-page identity, email local-part identity, fraternity/school/chapter context, and provenance type
  - no fraternity-specific allowlist was added
  - the same rules now cover Theta Chi, Sigma Chi, Alpha Gamma Rho, Lambda Chi Alpha, Beta Upsilon Chi, Delta Chi, and other fraternities in the same cleanup cohort

## Functional Requirements
| Requirement | Status | Evidence |
|---|---|---|
| `FR-G1` bounded cohort before broader mutation | passed | phase began with a dry-run-only classifier in [`PHASE_5_CONTACT_DRYRUN_2026-04-09.json`](./PHASE_5_CONTACT_DRYRUN_2026-04-09.json) before any apply |
| `FR-G2` phase ends with written report and approval gate | passed | this report plus [`PHASE_5_CONTACT_APPLY_2026-04-09.json`](./PHASE_5_CONTACT_APPLY_2026-04-09.json) and [`PHASE_5_CONTACT_FINAL_2026-04-09.json`](./PHASE_5_CONTACT_FINAL_2026-04-09.json) |
| `FR-G3` accepted, rejected, and unresolved samples included | passed | see sample tables below |
| `FR-G6` accepted contact must have explainable provenance | passed | accepted rows now carry `supportingPageUrl`, `supportingPageScope`, `contactProvenanceType`, `reasonCode`, and `decisionStage = legacy_contact_reconciliation` |
| `FR-G7` every decision attributable to a stage, rule, and evidence source | passed | all apply mutations wrote accepted or rejected provenance entries into `chapters.contact_provenance` |
| `FR-5.1` email only counts when the supporting page is chapter-safe | passed | accepted email rows are now limited to `chapter_site`, `school_affiliation_page`, or `nationals_chapter_page` with identity guards |
| `FR-5.2` nationals-generic email never populates chapter email | passed | Theta Chi HQ and similar HQ contact remain rejected with `contactProvenanceType = national_generic` or `ambiguous` |
| `FR-5.3` Instagram only counts when the page or handle has chapter-specific identity | passed | accepted Instagram rows are chapter-specific school, chapter-site, or chapter-specific national-page handles |
| `FR-5.4` school-wide and nationals-wide handles are rejected unless clearly chapter-specific | passed | generic school pages and generic HQ handles were cleared; ambiguous national handles remain review-only |
| `FR-5.7` accepted contact stores supporting page, scope, provenance, and reason | passed | see live spot-check samples below |
| `FR-5.8` chapter-specific national-page contact is labeled as such | passed | accepted BYX and Lambda Chi rows now use `contactProvenanceType = national_specific_to_chapter` |

## Before / After Metrics
| Metric | Before Phase 5 | Pre-Apply Final Dry Run | After Apply Final Verification |
|---|---:|---:|---:|
| Rows with `contact_email` present | `409` | n/a | `151` |
| Rows with `instagram_url` present | `561` | n/a | `397` |
| Rows with untrusted email provenance | `409` | n/a | `110` |
| Rows with untrusted Instagram provenance | `561` | n/a | `116` |
| Dry-run rows to accept | n/a | `317` | `0` |
| Dry-run rows to reject | n/a | `387` | `0` |
| Dry-run rows still requiring review | n/a | `142` | `211` |
| Trusted email rows after apply | n/a | n/a | `41` |
| Trusted Instagram rows after apply | n/a | n/a | `281` |

## Why Review Count Increased After Apply
- The pre-apply dry run counted rows by the strongest action available on that row.
- Many rows had one field that could be safely accepted or cleared and another field that was still ambiguous.
- After apply, the resolved field dropped out of the action set, so the same row now appears only in the remaining review bucket.
- That is why the final verification shows `211` review rows even though the pre-apply dry run showed `142` review rows.

## Accepted Samples
| Chapter | Field | Supporting Page | Scope | Outcome | Why it is safe |
|---|---|---|---|---|---|
| `alpha-phi-active-south-dakota-state-university` | `contact_email = alphagammarhosdsu@gmail.com` | `https://sdstateagr.com/contact` | `chapter_site` | accepted | standalone fraternity host with Alpha Gamma Rho identity and chapter-local contact page |
| `james-madison-university-james-madison-university` | `contact_email = jmu@byx.org` | `https://byx.org/join-a-chapter/` | `nationals_chapter_page` | accepted | chapter-specific national page entry with school-specific chapter contact |
| `university-of-central-oklahoma-university-of-central-oklahoma` | `contact_email = ucobyx@gmail.com` | `https://byx.org/join-a-chapter/` | `nationals_chapter_page` | accepted | chapter-specific national page entry with school-specific contact |
| `16127` | `instagram_url = https://www.instagram.com/lambdachi_indstate` | `https://www.lambdachi.org/chapters/iota-epsilon-indiana-state/` | `nationals_chapter_page` | accepted | chapter-specific nationals page explicitly tied to Indiana State |
| `alpha-omega-active-murray-state-university` | `instagram_url = https://www.instagram.com/agralphaomega` | `https://www.murraystate.edu/campus/orgsRecreation/StudentOrganizations/greek/organizations.aspx` | `school_affiliation_page` | accepted | official school page plus chapter-specific handle with fraternity and chapter identity |
| `kappa-rho-american-university` | `instagram_url = https://www.instagram.com/ausigmachi` | `https://www.instagram.com/ausigmachi/` | `chapter_site` | accepted | direct chapter Instagram page, normalized to the actual handle URL instead of the login redirect |

## Rejected Samples
| Chapter | Field | Previous Value | Supporting Page | Outcome | Why it was rejected |
|---|---|---|---|---|---|
| `delta-omega-university-of-tulsa` | `contact_email` | `sga.ssc@utulsa.edu` | `https://utulsa.edu/student-life/student-organizations/` | cleared | generic school organization page with no local chapter identity |
| `delta-omega-university-of-tulsa` | `instagram_url` | `https://www.instagram.com/tulsasigmachi` | `https://utulsa.edu/student-life/student-organizations/` | cleared | same generic school page, no safe supporting identity for this legacy match |
| `psi-psi-syracuse-university` | `contact_email` | `scpsscheduling@syr.edu` | `https://experience.syracuse.edu/student-engagement/about/contact/` | cleared | university scheduling/contact mailbox, not chapter-specific contact |
| `zeta-psi-university-of-cincinnati` | `contact_email` | `heidi.pettyjohn@uc.edu` | `https://www.uc.edu/about/digital-accessibility/contact-support/report-concern.html` | cleared | false positive caused by two-letter `SC` identity leakage, fixed mid-phase |
| `gamma-eta-bucknell-university` | `instagram_url` | `https://www.instagram.com/rider_university` | prior Rider Theta Chi support page | already clean and remains clean | wrong-school university Instagram was previously quarantined and remains removed |
| `zeta-gamma-university-of-alberta` | `contact_email` and `instagram_url` | `ihq@thetachi.org`, `https://www.instagram.com/thetachiihq` | `https://www.thetachi.org/chapters` | already clean and remains clean | HQ-only nationals contact is still blocked from counting as chapter contact |

## Unresolved Samples
| Chapter | Field | Current Value | Supporting Page | Why it remains review |
|---|---|---|---|---|
| `alpha-miami-university` | `contact_email` | `clarkorj@MiamiOH.edu` | `https://www.miamiohifc.org/sigma-chi` | could be a chapter officer or a generic campus mailbox; URL is chapter-specific but email identity is ambiguous |
| `mu-lambda-sacred-heart-university` | `contact_email` | `martinz3@mail.sacredheart.edu` | `https://www.sacredheart.edu/.../sigma-chi/` | school page is chapter-specific, but the email looks like a personal/student mailbox without enough role context |
| `omicron-gamma-florida-atlantic-university` | `contact_email` | `ognupes@gmail.com` | `https://www.fau.edu/fslife/about/chapters/fraternities/kappa-alpha-psi/` | plausible chapter contact, but it still needs a stronger role or page-context signal |
| `f26afb30-7d91-467b-9792-ab19e69aa757` | `instagram_url` | `https://www.instagram.com/fijiindiana` | `https://zetafiji.com/` | chapter site exists, but the stored handle still needs stronger support than the current page fetch provides |
| `e6cee197-1aa3-4ee8-8595-144453861dff` | `instagram_url` | `https://www.instagram.com/lambdachialphaku` | `https://www.lambdachi.org/chapters/zeta-iota-kansas/` | likely correct, but the national page evidence remains slightly under-specified for auto-accept |
| `8f54f267-3a01-4b96-a64c-c366a5a39ac1` | `contact_email` | `lbeckham1@my.apsu.edu` | `https://www.apsu.edu/greek-life/fraternities/sigma-chi.php` | official school page with chapter identity, but personal student email still needs role-level proof |

## Top Failure Modes Still Remaining
| Remaining class | Symptoms | Why it is unresolved |
|---|---|---|
| personal or student mailbox on a chapter-specific school page | `mail.sacredheart.edu`, `my.apsu.edu`, `@ku.edu` on chapter pages | likely valid in some cases, but too risky to auto-accept without role/officer context |
| chapter-specific national page with a plausible school email | school mailbox or campus alias listed on a national chapter card | page proves chapter existence but not always the role or freshness of the email |
| chapter-site or direct social page with thin fetchable context | site exists but HTML fetch is sparse or normalized content is minimal | not enough on-page evidence to prove the handle belongs to the target chapter without manual review |
| historic or low-shape legacy rows that still carry one plausible value | one field looks good, the other is clearly bad or ambiguous | phase chose precision over forcing a guess |

## False-Positive Risk Review
- Biggest risk if we widen automatically now:
  - personal or student email addresses on chapter-specific school or national pages could still be outdated, officer-specific, or unrelated to a durable chapter contact channel
  - some direct Instagram handles on national chapter directories look correct but are not yet supported by enough fetched page context to auto-accept safely
- What this phase did to protect precision:
  - rejected generic school-office contact even when it appeared on school pages
  - rejected false positives created by two-letter fraternity initials like `SC`
  - normalized standalone fraternity hosts into `chapter_site` instead of misclassifying them as school pages
  - left ambiguous rows untouched instead of forcing them into accepted data

## Exact Recommendation For Next Phase
- Do not widen throughput yet.
- The next bounded slice should target only the `211` review rows and subdivide them into:
  - `school_affiliation_page + personal/student email`
  - `nationals_chapter_page + plausible school email`
  - `chapter_site or nationals_chapter_page + plausible Instagram`
- The next generalized feature should be `role-context extraction`:
  - detect whether the page labels the person as president, recruitment chair, social chair, chapter email, chapter contact, or similar
  - accept only when the role context proves the mailbox or handle is actually chapter-specific

## Approval Request
- Phase 5 achieved the bounded objective:
  - unsafe legacy chapter contact was either removed or backfilled with trusted provenance
  - the known bad examples remain fixed
  - the live legacy contact population is now reduced to the intentionally unresolved review bucket
- If approved, the next phase should be a review-bucket-only pass, not a throughput expansion.
