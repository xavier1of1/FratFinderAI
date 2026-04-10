# Phase 7 Investor Readiness Report

Generated: `2026-04-09T23:47:53+00:00`

## Executive Read
- This packet distinguishes between `system complete` and `presentation safe` rows.
- `system complete` is the program metric used through the approval-gated plan.
- `presentation safe` is stricter and excludes rows with placeholder Instagram handles, generic org-portal websites, or other obviously weak visible fields.
- The curated accepted sample uses a `supporting page` column, because some strong rows are backed by official school or national chapter pages rather than a chapter-owned website.
- The environment is demo-safe only if we present the curated sample packet and avoid treating the whole dataset as uniformly investor-ready.

## Core Metrics
| Metric | Value |
|---|---:|
| Total chapters | 2534 |
| Active rows | 2486 |
| Inactive rows | 48 |
| System complete rows | 308 |
| Presentation-safe complete rows | 283 |
| Presentation gap | 25 |
| Active rows with any contact | 963 |
| Active rows with chapter-specific email | 40 |
| Active rows with chapter-specific Instagram | 270 |
| Nationals-only contact rows | 0 |
| Validated inactive rows (strict KPI) | 3 |

## Queue Snapshot
| Queue state | Count |
|---|---:|
| actionable | 1519 |
| deferred | 3237 |

## Presentation Risks Still In Data
| Risk category | Count |
|---|---:|
| Placeholder / broken Instagram handles | 0 |
| Generic org-portal websites still visible on rows | 21 |
| Generic school-office emails still visible on rows | 10 |
| Cross-school trusted email risks | 4 |

## Demo Recommendation
- Show curated rows from the sample packet only.
- If asked about coverage, say the system currently has `308` strict complete rows and `283` presentation-safe complete rows, with `963` active rows containing at least some contact signal.
- Do not claim a few thousand completed rows. That target is not met in the live environment.
- Lean on explainability, provenance, chapter-specific national-page handling, and inactive validation rather than raw coverage.

## Accepted Sample Preview
| Fraternity | Chapter slug | University | Safe email | Safe Instagram | Supporting page |
|---|---|---|---|---|---|
| alpha-gamma-rho | beta-phi-active-university-of-idaho | University of Idaho | agr.uidaho@gmail.com | https://www.instagram.com/uidahoagr | https://www.uidahoalphagammarho.net/ |
| theta-xi | alpha-sigma-bradley-university | Bradley University | thetaxi@lydia.bradley.edu |  | http://lydia.bradley.edu/campusorg/thetaxi/house.html |
| sigma-chi | theta-rho-illinois-state-university | Illinois State University | jaschw3@ilstu.edu | https://www.instagram.com/isu_sigmachi | https://redbirdlife.illinoisstate.edu/organization/sigmachifraternity |
| sigma-chi | omega-omega-university-of-arkansas | University of Arkansas |  | https://www.instagram.com/arkansas.sigmachi | https://uagreeks.uark.edu/interfraternity-council/sigma-chi.php |
| sigma-chi | nu-nu-columbia-university | Columbia University |  | https://www.instagram.com/sigmachi_columbia | https://www.cc-seas.columbia.edu/student-group/sigma-chi |
| sigma-chi | lambda-delta-university-of-california-merced | University of California-Merced |  | https://www.instagram.com/ucmsigmachi | https://fraternitysorority.ucmerced.edu/chapters-councils/fraternity-sorority-council/fraternity-chapters/sigma-chi |
| sigma-chi | beta-pi-oregon-state-university | Oregon State University |  | https://www.instagram.com/sigmachibetapi | https://sigmachioregonstate.squarespace.com |
| phi-gamma-delta | tau-deuteron-chapter-university-of-texas | University of Texas | betsy@texasfiji.com |  | http://texasfiji.com/ |
| phi-gamma-delta | tau-delta-chapter-university-of-texas-dallas | University of Texas Dallas |  | https://www.instagram.com/utdfiji | https://www.utdfiji.org/ |
| phi-gamma-delta | mu-deuteron-chapter-university-of-iowa | University of Iowa | ryan-m-witt@uiowa.edu | https://www.instagram.com/iowafiji | https://fsl.uiowa.edu/chapters-and-councils/phi-gamma-delta |
| phi-gamma-delta | lambda-omega-chapter-university-of-western-ontario | University of Western Ontario |  | https://www.instagram.com/fijiuwo | http://www.fijiuwo.com/ |
| phi-gamma-delta | lambda-iota-chapter-purdue-university | Purdue University |  | https://www.instagram.com/purdue.fiji | https://boilerlink.purdue.edu/organization/phigammadelta |
| phi-gamma-delta | lambda-chapter-depauw-university | DePauw University |  | https://www.instagram.com/lambdafiji | http://www.depauwfiji.com |
| phi-gamma-delta | gamma-deuteron-chapter-knox-college | Knox College | jrwilliams@knox.edu | https://www.instagram.com/knox_fiji | https://www.knox.edu/campus-life/clubs-and-organizations/phi-gamma-delta |
| phi-gamma-delta | delta-chapter-bucknell-university | Bucknell University |  | https://www.instagram.com/bucknellfiji | https://fijibucknell.wordpress.com/ |

## Validated Inactive Preview
| Fraternity | Chapter slug | University | Source type | Evidence URL |
|---|---|---|---|---|
| sigma-chi | beta-iota-university-of-oregon | University of Oregon | official_school | https://gogreek.oregonstate.edu/scorecards |
| chi-psi | pi-tau-south-dakota-school-of-mines-and-technology | South Dakota School of Mines and Technology | official_school | https://fraternitysorority.mines.edu/inter-fraternity-council/ |
| delta-kappa-epsilon | delta-kappa-university-of-pennsylvania | University of Pennsylvania | official_school | https://ofsl.universitylife.upenn.edu/chapters/ |

## Unresolved Preview
| Fraternity / Source | Chapter slug | Field | Outcome | Queue state | Queries attempted |
|---|---|---|---|---|---:|
| alpha-delta-phi / alpha-delta-phi-main | cumberland-cumberland-university | find_instagram | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | deferred | 0 |
| alpha-tau-omega / alpha-tau-omega-main | epsilon-zeta-louisiana-state | find_website | No candidate website URL available; search preflight degraded | deferred | 0 |
| alpha-tau-omega / alpha-tau-omega-main | epsilon-zeta-louisiana-state | find_instagram | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | deferred | 0 |
| alpha-gamma-rho / alpha-gamma-rho-main | west-virginia-name-college | find_instagram | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | deferred | 0 |
| alpha-gamma-rho / alpha-gamma-rho-main | west-virginia-name-college | find_website | No candidate website URL available; search preflight degraded | deferred | 0 |
| alpha-gamma-rho / alpha-gamma-rho-main | idaho-name-college | find_instagram | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | deferred | 0 |
| alpha-gamma-rho / alpha-gamma-rho-main | idaho-name-college | find_website | No candidate website URL available; search preflight degraded | deferred | 0 |
| delta-sigma-phi / delta-sigma-phi-main | eta-beta-california-state-san-bernardino | find_website | No candidate website URL available; search preflight degraded | deferred | 0 |
| delta-sigma-phi / delta-sigma-phi-main | eta-beta-california-state-san-bernardino | find_instagram | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | deferred | 0 |
| alpha-tau-omega / alpha-tau-omega-main | iota-sigma-central-missouri | find_instagram | No candidate instagram URL found in provenance, chapter website, or search results; search preflight degraded | deferred | 0 |

## Top Risks To Avoid Showing
| Fraternity | Chapter slug | University | Risk reasons |
|---|---|---|---|
| alpha-chi-rho | chapter-map |  | generic_national_instagram, suspicious_identity |
| alpha-delta-gamma | 2fusf |  | suspicious_identity |
| alpha-delta-gamma | alpha |  | suspicious_identity |
| alpha-delta-gamma | alpha-beta |  | suspicious_identity |
| alpha-delta-gamma | alpha-delta |  | suspicious_identity |
| alpha-delta-gamma | alpha-epsilon |  | suspicious_identity |
| alpha-delta-gamma | alpha-eta |  | suspicious_identity |
| alpha-delta-gamma | alpha-gamma |  | suspicious_identity |
| alpha-delta-gamma | alpha-iota |  | suspicious_identity |
| alpha-delta-gamma | alpha-kappa |  | suspicious_identity |
| alpha-delta-gamma | alpha-lambda |  | suspicious_identity |
| alpha-delta-gamma | alpha-theta |  | suspicious_identity |
| alpha-delta-gamma | alpha-zeta |  | suspicious_identity |
| alpha-delta-gamma | beta |  | cross_school_email, suspicious_identity |
| alpha-delta-gamma | chi |  | suspicious_identity |

## Final Call
- Demo-ready for a curated, accuracy-first investor walkthrough.
- Not demo-ready for unrestricted browsing of all populated rows.