# Phase 3/5 Approval Report

## Goal

Remove clearly wrong legacy data without harming acceptable chapter-specific records.

This bounded phase targeted:
- generic nationals email and Instagram pollution in chapter rows
- school-office email pollution in chapter rows
- obvious wrong-school or junk website pollution
- obviously invalid non-chapter rows that should not exist in the chapter table

## Generalized Solution

The solution was generalized in four ways instead of being Theta Chi-specific:

1. `Legacy cleanup is provenance-aware`
- Cleanup writes now record a rejection provenance entry with:
  - `decisionStage = legacy_contact_quarantine`
  - `reasonCode`
  - `previousValue`
  - `supportingPageUrl`
  - `supportingPageScope`
- This means we do not silently erase data. We keep an audit trail explaining why it was rejected.

2. `Generic nationals contact is treated as a chapter-data anti-signal`
- Chapter rows now scrub legacy contact when it is clearly:
  - the fraternity HQ email
  - the fraternity HQ Instagram
  - a generic national handle like `hq`, `ihq`, or `national`
- Chapter-specific national pages are still preserved as acceptable website support when they are actually chapter-specific.

3. `School-office contact is treated separately from chapter contact`
- Cleanup now removes legacy school-office emails such as:
  - `graduateprograms@...`
  - `fsl@...`
  - `greeklife@...`
  - `info@...`
- This rule is generic and based on office-email shape plus missing chapter-specific evidence, not on school-specific allowlists.

4. `Invalid rows and wrong-school websites are judged by reusable semantics`
- Invalid chapter deletion only applies to true invalid classes:
  - `invalid_entity_legacy`
  - `ranking_or_report_row`
  - `year_or_percentage_as_identity`
  - `other_greek_organization_row`
  - `expansion_or_installment_row`
  - similar clearly non-chapter classes
- It does not delete semantically incomplete but potentially repairable rows.
- Wrong-school websites are scrubbed only when official-domain verification gives a hard reject such as `missing_target_school_context`.

## Validation

Passed:
- `python -m pytest services/crawler/src/fratfinder_crawler/tests/test_precision_tools.py services/crawler/src/fratfinder_crawler/tests/test_field_jobs_engine.py services/crawler/src/fratfinder_crawler/tests/test_normalizer.py -q`
- `pnpm.cmd --filter @fratfinder/web typecheck`

## Before / After

| Metric | Before | After | Delta | Notes |
|---|---:|---:|---:|---|
| Total chapters | 5,576 | 2,534 | -3,042 | Deleted only clearly invalid non-chapter rows |
| Invalid rows eligible for deletion | 3,042 | 0 | -3,042 | `baseball`, `law`, `engineering`, `%` rows, award rows, etc. |
| Theta Chi HQ polluted rows | 127 | 0 | -127 | `ihq@thetachi.org` and `@thetachiihq` removed from chapter rows |
| Bryant / Missouri school-office examples remaining | 3 | 0 | -3 | `graduateprograms@bryant.edu`, `fsl@missouri.edu` scrubbed |
| Rider mismatch rows remaining | 2 | 0 | -2 | wrong-school Rider website / Rider Instagram cleared |
| Rows touched with cleanup provenance | 0 | 1,032 | +1,032 | Every cleanup action is explainable in-row |
| Remaining invalid-entity rows | 3,042 | 0 | -3,042 | No all-invalid legacy rows remain |
| Residual website-only cleanup candidates | 680 before verifier fix | 94 | -586 | Stopped here for review because this remaining cohort has false-positive risk |

## Accepted Samples

These rows were preserved because the website is still acceptable or the contact looked chapter-specific.

| Chapter | Why It Was Preserved | Current State |
|---|---|---|
| `zeta-gamma-university-of-alberta` | `https://www.thetachi.org/zeta-gamma` is a chapter-specific national page, which is acceptable as the website | Website kept, HQ email removed, HQ Instagram removed |
| `kappa-chi-chapter-william-woods-college` | `@wwufiji` survived because the `Fiji` alias plus authoritative context was treated as chapter-specific, not national-generic | Instagram kept |
| `gamma-omega-vanderbilt-university` | `@vandythetachi` was preserved because it is local chapter identity even though the old Rider website was removed | Instagram kept, website removed |

## Rejected / Cleared Samples

| Chapter | Bad Value | Cleanup Reason | Result |
|---|---|---|---|
| `zeta-gamma-university-of-alberta` | `ihq@thetachi.org` | `legacy_email_failed_chapter_specificity` | email cleared |
| `zeta-gamma-university-of-alberta` | `https://www.instagram.com/thetachiihq` | `legacy_nationals_generic_contact` | Instagram cleared |
| `gamma-eta-bucknell-university` | Rider Theta Chi page | `legacy_website_failed_official_verification` | website cleared |
| `gamma-eta-bucknell-university` | `https://www.instagram.com/rider_university` | `legacy_instagram_failed_chapter_specificity` | Instagram cleared |
| `zeta-chi-bryant-university` | `graduateprograms@bryant.edu` | `legacy_email_failed_chapter_specificity` | email cleared |
| `phi-sigma-bryant-university` | `graduateprograms@bryant.edu` | `legacy_email_failed_chapter_specificity` | email cleared |
| `missouri-alpha-a-university-of-missouri` | `fsl@missouri.edu` | `legacy_email_failed_chapter_specificity` | email cleared |
| `baseball-basketball` | fake chapter row | `ranking_or_report_row` | row deleted |

## Residual Website Cohort

I did **not** bulk-apply the remaining 94 website-only cleanup candidates. This was intentional.

The reason is that the residual cohort is mixed:
- some are clearly wrong and should be removed
- some are plausible school FSL / organization pages and need a more careful generalized website rule before bulk mutation

### Examples that look clearly wrong

| Chapter | Website | Why It Still Looks Wrong |
|---|---|---|
| `delta-chi-lenoir-rhyne-university` | `https://gordie.studenthealth.virginia.edu/connect/memorials/harrison-kowiak` | unrelated Virginia memorial page |
| `theta-zeta-university-of-north-carolina-asheville` | `https://docsouth.unc.edu/true/grant/grant.html` | archival document page, not chapter site |
| `nu-omega-chapter-university-of-oklahoma` | Google KML map export | machine asset, not a chapter website |

### Examples that are ambiguous enough to stop at the approval gate

| Chapter | Website | Why I Did Not Auto-Clear Yet |
|---|---|---|
| `gamma-xi-university-of-wyoming` | `https://www.uwyo.edu/fsl/aboutus/chapter-page/sigma-chi.html` | could be a valid school-affiliation chapter page |
| `beta-upsilon-active-university-of-delaware` | `https://sites.udel.edu/agr/` | could be a valid chapter-maintained school-hosted page |
| `alpha-theta-active-university-of-maryland-college-park` | `https://terplink.umd.edu/organization/alpha-gamma-rho` | could be a valid campus organization profile |

This is the correct place to stop and ask for approval before Phase 4 expands website cleanup further.

## Top Failure Modes Found In This Phase

| Failure Mode | What Caused It | Generalized Fix |
|---|---|---|
| HQ chapter pollution | national email/Instagram copied into chapter rows | quarantine on national-generic identity and national-profile equality |
| School-office email pollution | school support addresses mistaken as chapter contact | generic-office email rejection with chapter-specificity requirement |
| Wrong-school website pollution | same-fraternity pages from the wrong campus | official-domain verification plus target-school host/context checks |
| Wikipedia junk chapter creation | university rankings, departments, sports, awards, years, demographics mis-entered as chapters | stricter semantic invalid-row deletion only for true invalid classes |
| Trailing-slash Instagram handle weakness | handle parsing lost identity when URL ended in `/` | centralized handle normalization |

## False-Positive Risk Review

What worked safely:
- chapter-specific national pages were preserved
- chapter-specific Instagram aliases like `Fiji` were preserved when context supported them
- known wrong rows from your examples were scrubbed correctly

What still needs a tighter generalized rule before broader rollout:
- school-hosted organization pages versus generic FSL directories
- student-organization portals versus actual chapter websites
- school subdomains with short aliases that are correct but not obviously tied to the school name

## Recommendation For Next Phase

Proceed to the next approval-gated phase only for:
- `website resolution refinement on the residual 94-row website cohort`

Do **not** widen to throughput work yet.

The next bounded phase should:
- separate `school chapter profile` from `generic FSL landing page`
- accept school-hosted chapter profile pages when they are chapter-specific
- reject school-hosted generic landing pages when they are only directory-level
- keep sample review on every batch before any broader cleanup

## Approval Request

This phase is ready for review.

What is now true in live data:
- your explicit HQ-contact examples were corrected
- your Bryant and Missouri office-email examples were corrected
- your Bucknell Rider mismatch was corrected
- the obvious invalid junk rows were removed

What I am intentionally **not** doing yet:
- bulk-clearing the remaining 94 website candidates until you approve the next website-specific refinement slice
