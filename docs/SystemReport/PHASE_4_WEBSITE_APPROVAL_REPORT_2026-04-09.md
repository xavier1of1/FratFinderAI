# Phase 4 Website Approval Report

## Goal
- Targeted problem: residual legacy website rows were still storing generic school landing pages, wrong-school pages, dead pages, and generic school org-portal roots as chapter websites.
- Phase: bounded Phase 4 website-specific cleanup and verifier hardening.
- Cohort size: `161` residual website rows at phase entry, plus `7` additional generic org-portal root rows surfaced after a verifier bug fix.

## Generalized Solution
- Reusable logic added:
  - deep-verify candidate pages directly instead of trusting only provenance snippets
  - distinguish chapter-specific school profile pages and org-profile pages from generic FSL/IFC/campus portal landing pages
  - classify generic org-portal roots like `/engage`, `/club_signup`, `/organizations`, and `/organization` as `generic_school_directory`
  - preserve true chapter-specific school pages such as `/chapter-page/<fraternity>`, `/project/<chapter>`, and `/organization/<fraternity>`
- Why this is generalized rather than fraternity-specific:
  - the new rules are based on page scope, path shape, host ownership, school match, fraternity identity, and chapter identity
  - no fraternity-specific allowlist was added
  - the same rules now cover UWYO FSL pages, Delaware school project pages, TAMUK IFC pages, TerpLink org pages, and CampusLabs/club-signup roots across multiple fraternities
- Subsystems touched:
  - Source Discovery and Page Targeting
  - Website Resolution
  - Queue Orchestration, Metrics, and Feedback Reporting

## Functional Requirements
| Requirement | Status | Evidence |
|---|---|---|
| `FR-G1` bounded cohort before broader mutation | passed | phase started from [`PHASE_4_RESIDUAL_WEBSITE_DRYRUN_2026-04-09.json`](./PHASE_4_RESIDUAL_WEBSITE_DRYRUN_2026-04-09.json) before apply |
| `FR-G6` accepted website values must have explainable provenance/evidence | passed | preserved sample rows remain only when the candidate page itself carries school/fraternity/chapter identity |
| `FR-G7` every decision attributable to stage/rule/evidence | passed | cleared rows now store `legacy_contact_quarantine` provenance with `reasonCode`, `previousValue`, and `supportingPageUrl` |
| `FR-4.2` website acceptance requires correct fraternity, school, and chapter-specific evidence | passed | UWYO, TerpLink, TAMUK, Delaware, and MIT chapter-site survivors stayed intact |
| `FR-4.3` reject wrong-school, generic-national, school-wide, and unrelated pages | passed | Geneseo-for-NYU, Trinity homepage, Alabama OFSL about page, and CampusLabs roots were cleared |
| `FR-4.4` separate true no-site outcomes from bad-site pollution | partial | bad website values were removed cleanly; broader `confirmed_absent` promotion remains for later website/email follow-up |
| `FR-4.6` accepted websites store page-scope and evidence-ready context | partial | new runtime writes do this; legacy good rows still show state/provenance drift and need later reconciliation |

## Before / After Metrics
| Metric | Before | After | Delta | Notes |
|---|---:|---:|---:|---|
| Residual website rows in initial bounded cohort | 161 | 0 | -161 | Initial dry run identified `161` safe website quarantines |
| Residual rows after first apply pass | 1 | 0 | -1 | `16127` at Indiana State exposed a verifier classification gap |
| Additional generic org-portal root rows surfaced by verifier fix | 7 | 0 | -7 | CampusLabs / `club_signup` / campus org-root pages were newly classified safely |
| Total legacy website rows cleared in this phase | 0 | 168 | +168 cleared | `161` initial clears plus `7` additional portal-root clears |
| Final residual website quarantine count | 161 | 0 | -161 | Final post-check is zero in [`PHASE_4_RESIDUAL_WEBSITE_FINAL_2026-04-09.json`](./PHASE_4_RESIDUAL_WEBSITE_FINAL_2026-04-09.json) |
| Targeted validation suites | 0 failing | 0 failing | n/a | `test_precision_tools`, `test_field_jobs_engine`, and `test_normalizer` all passed |
| Live rows with a website value but non-`found` website state | 382 | 382 | 0 | Remaining legacy state drift, intentionally not bulk-mutated in this slice |
| Live rows with a website value but no website provenance entry | 714 | 714 | 0 | Remaining provenance gap for later consistency work |

## Accepted Samples
| Request / Source | Chapter slug | Field | Supporting page | Page scope | Specificity | Result | Why accepted |
|---|---|---|---|---|---|---|---|
| `sigma-chi-main` | `gamma-xi-university-of-wyoming` | `website_url` | `https://www.uwyo.edu/fsl/aboutus/chapter-page/sigma-chi.html` | `school_affiliation_page` | `chapter_specific` | kept | School-owned chapter profile page explicitly names Sigma Chi and University of Wyoming |
| `alpha-gamma-rho-main` | `alpha-theta-active-university-of-maryland-college-park` | `website_url` | `https://terplink.umd.edu/organization/alpha-gamma-rho` | `school_affiliation_page` | `chapter_specific` | kept | TerpLink org page is school-owned and embedded HTML/JSON carries fraternity identity tied to UMD |
| `delta-chi-main` | `delaware-delaware` | `website_url` | `https://sites.udel.edu/fsll/project/delta_chi/` | `school_affiliation_page` | `chapter_specific` | kept | School-hosted project page is chapter-specific, not a generic FSL landing page |
| `sigma-chi-main` | `zeta-pi-texas-a-m-university-kingsville` | `website_url` | `https://www.tamuk.edu/greeks/ifc/sigma-chi.html` | `school_affiliation_page` | `chapter_specific` | kept | TAMUK IFC chapter page directly matches fraternity and school |
| `alpha-delta-phi-main` | `lambda-phi-massachusetts-institute` | `website_url` | `https://adp.mit.edu` | `chapter_site` | `chapter_specific` | kept | Independent chapter site with direct MIT chapter identity |

## Rejected Samples
| Request / Source | Chapter slug | Field | Supporting page | Page scope | Result | Why rejected |
|---|---|---|---|---|---|---|
| `theta-chi-main` | `upsilon-new-york-university` | `website_url` | `https://www.geneseo.edu/inter_greek_council/theta-chi/` | `unrelated` | cleared to `missing` | Wrong school: SUNY Geneseo page was being used for an NYU chapter |
| `alpha-gamma-rho-main` | `beta-omicron-active-university-of-wyoming` | `website_url` | `https://www.uwyo.edu/fsl/aboutus/chapter_page/alpha-gamma-rho.html` | `unrelated` | cleared to `missing` | Candidate resolves to a broken UWYO error page, not a usable chapter site |
| `alpha-delta-phi-main` | `phi-kappa-trinity-college` | `website_url` | `https://www.trincoll.edu/` | `generic_school_root` | cleared to `missing` | Generic school homepage carried no chapter identity |
| `lambda-chi-alpha-main` | `16127` | `website_url` | `https://indstate.campuslabs.com/engage` | `generic_school_directory` | cleared to `missing` | CampusLabs org-portal root is a generic school directory landing page, not a chapter website |
| `alpha-tau-omega-main` | `kappa-psi-pittsburgh` | `website_url` | `https://experience.pitt.edu/club_signup?group_type=86573` | `generic_school_directory` | cleared to `missing` | Generic school club-signup portal page lacks chapter-specific website identity |
| `sigma-alpha-epsilon-main` | `alabama-mu-mother-mu-university-of-alabama` | `website_url` | `https://ofsl.sl.ua.edu/about/` | `generic_school_directory` | cleared to `missing` | FSL department "About" page is school-level admin content, not chapter-specific |

## Unresolved Samples
| Request / Source | Chapter slug | Field | Current state | Blocking reason | Next action |
|---|---|---|---|---|---|
| `alpha-delta-phi-main` | `lambda-phi-massachusetts-institute` | `website_url` | website preserved but field state still legacy-drifted | Historical row still has state/provenance inconsistency even though the website is valid | Later state/provenance reconciliation pass |
| `sigma-chi-main` | `beta-chi-emory-university` | `contact_email` | `community@large.foster` still present | Website was cleaned, but this phase did not mutate legacy email values | Phase 5 email specificity cleanup |
| `alpha-gamma-rho-main` | `beta-upsilon-active-university-of-delaware` | `contact_email` | `alphagammarhoud@gmail.com.phone` still present | Website was cleaned, but malformed email normalization is out of scope for this website slice | Phase 5 email cleanup and normalization |
| `sigma-alpha-epsilon-main` | `alabama-mu-mother-mu-university-of-alabama` | `instagram_url` | Instagram retained while website/email were cleared | This phase only targeted website pollution; surviving social handles still need chapter-specific review | Phase 5 Instagram specificity cleanup |

## Top Failure Modes
| Failure mode | Count | Cause | Effect | Mitigation |
|---|---:|---|---|---|
| Generic school FSL/IFC/recognized-chapter landing pages stored as chapter websites | 161 | Legacy rows kept school department or IFC landing pages because they looked school-adjacent and fraternity-relevant | Bad websites made chapters look more complete than they really were and could unlock later bad email/Instagram decisions | Candidate-page verification plus generic school directory/root rejection |
| Generic school org-portal roots misclassified as weak websites | 7 | CampusLabs / `club_signup` / org-root pages rejected only as low confidence instead of a safe generic-directory class | Residual bad websites survived the first apply pass | Added generic org-portal root classification and reran the same cohort |
| Website state drift on surviving valid website rows | 382 | Historical rows can still have a real website value while `field_states.website_url` is not `found` | Operator UI and downstream logic can misread a row's true completeness | Dedicated later consistency/state reconciliation pass |
| Website values with no website provenance entry | 714 | Older writes predate the shared evidence contract or never backfilled cleanly | Good rows are harder to audit and reason about | Dedicated provenance backfill after website/contact rules stabilize |

## False-Positive Risk Review
- Biggest false-positive risk: over-clearing real chapter pages that live on school-owned org platforms or school subpaths.
- Guardrail added:
  - candidate page fetch and final-URL verification
  - chapter-specific school path markers like `/chapter-page/`, `/project/`, and `/organization/<fraternity>`
  - fraternity + school + chapter-context checks before clearing
- Remaining ambiguity:
  - legacy rows that already have a real website but stale `field_states` / missing provenance are still present
  - this phase intentionally did not bulk mutate those rows because that needs a broader state-consistency rule, not another quarantine rule

## Recommendation For Next Phase
- Safe next step: bounded Phase 5 cleanup focused on chapter-specific `email` and `instagram` acceptance for rows whose website is now clean or intentionally missing.
- Risk if widened too early: if email/Instagram cleanup is broadened without the same provenance care, we could erase valid chapter-local contact or keep national/school-admin contact that only looked plausible.

## Approval Request
- Ready for approval: yes
- Requested feedback:
  - review the preserved website sample set and confirm the accepted school-hosted chapter pages match your expectations
  - review the unresolved email examples, especially `beta-chi-emory-university` and `beta-upsilon-active-university-of-delaware`, because those are strong Phase 5 targets
