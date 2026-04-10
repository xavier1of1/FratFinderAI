# Phase 0 Baseline

## Goal
- Lock the live baseline before the later accuracy-recovery mutations widen.

## Locked Definitions
- `complete_row`: active chapter plus at least one accurate chapter-supported email or Instagram.
- `chapter_specific_contact_row`: chapter row supported by `chapter_specific`, `school_specific`, or `national_specific_to_chapter` evidence.
- `nationals_only_contact_row`: chapter row whose present contact data is only supported by `national_generic` evidence.
- `inactive_validated_row`: inactive chapter with school/activity validation evidence.
- `confirmed_absent_website_row`: website intentionally resolved absent rather than merely missing.

## Baseline Metrics
| Metric | Value |
|---|---:|
| Total chapters | 5576 |
| Complete rows | 0 |
| Chapter-specific contact rows | 0 |
| Nationals-only contact rows | 0 |
| Validated inactive rows | 35 |
| Confirmed-absent website rows | 2 |
| Active rows with chapter email | 0 |
| Active rows with chapter Instagram | 0 |
| Active rows with any contact | 2599 |

## Queue Snapshot
| Metric | Value |
|---|---:|
| Queued field jobs | 5160 |
| Actionable field jobs | 1975 |
| Deferred field jobs | 3185 |
| Running field jobs | 0 |
| Updated field jobs | 2041 |
| Review-required field jobs | 370 |
| Terminal-no-signal field jobs | 550 |

## Nationals Profile Coverage
| Metric | Value |
|---|---:|
| Nationals profiles | 21 |
| Nationals with email | 0 |
| Nationals with Instagram | 0 |
| Nationals with phone | 0 |

## Instrumentation Gaps
| Metric | Value |
|---|---:|
| Inactive chapter rows | 46 |
| Rows needing provenance backfill | 1721 |

## Top Evidence Reason Codes
| Reason code | Count |
|---|---:|
| unknown | 6979 |
| fraternity_absent_from_official_school_list | 53 |
| chapter_already_inactive | 11 |

## Locked Cohort Bounds
- Analysis cohort size: `25`
- Write cohort size: `100`
- Accepted / rejected / unresolved sample pack target: `10 / 10 / 10`
- Rollback rule: stop widening if `nationals_only_contact_row` increases or accepted-sample precision regresses.

## Notes
- These metrics are now backed by first-class `contact_provenance` instrumentation where available.
- Legacy chapter rows without the new provenance envelope are counted conservatively and surfaced as `rows needing provenance backfill`.