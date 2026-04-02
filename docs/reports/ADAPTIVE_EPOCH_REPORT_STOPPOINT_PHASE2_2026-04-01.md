# Adaptive Train/Eval Epoch Report (2026-04-02 01:01:17 UTC)

- Epochs: `1`
- Adaptive runtime: `adaptive_assisted`
- Train sources: `sigma-chi-main`
- Eval sources: `delta-sigma-phi-main`

## KPI Delta Slope (Adaptive - Legacy)
- recordsPerPageDeltaSlope: `0.0`
- pagesPerRecordDeltaSlope: `0.0`
- upsertRatioDeltaSlope: `0.0`
- jobsPerMinuteDeltaSlope: `0.0`
- reviewRateDeltaSlope: `0.0`
- anyContactRateDeltaSlope: `0.0`
- balancedScoreSlope: `0.0`

## Per-Epoch KPI Deltas
| Epoch | Records/Page Delta | Pages/Record Delta | Upsert Ratio Delta | Jobs/Min Delta | Review Rate Delta | Any Contact Delta | Balanced Score |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |

## Raw Rows
```json
[
  {
    "epoch": 1,
    "policyVersion": "adaptive-v1",
    "runtimeMode": "adaptive_assisted",
    "train": {
      "sourceCount": 1.0,
      "pages_processed": 0.0,
      "records_seen": 0.0,
      "records_upserted": 0.0,
      "review_items_created": 1.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 6.334,
      "jobs_per_minute": 0.0,
      "run_ids": [
        183
      ]
    },
    "replay": {
      "eventsApplied": 35,
      "windowDays": 7,
      "batchSize": 500,
      "runtimeMode": "adaptive_assisted",
      "sourceCount": 1
    },
    "evalLegacy": {
      "sourceCount": 1.0,
      "pages_processed": 0.0,
      "records_seen": 0.0,
      "records_upserted": 0.0,
      "review_items_created": 1.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 6.295,
      "jobs_per_minute": 0.0,
      "run_ids": [
        184
      ],
      "enrichment": {
        "processed": 0,
        "requeued": 0,
        "failed_terminal": 0,
        "skipped_provider_degraded": 1
      },
      "coverage": {
        "chapters": 0,
        "any_contact": 0,
        "website": 0,
        "email": 0,
        "instagram": 0,
        "all_three": 0,
        "any_contact_rate": 0.0,
        "website_rate": 0.0,
        "email_rate": 0.0,
        "instagram_rate": 0.0,
        "all_three_rate": 0.0
      }
    },
    "evalAdaptive": {
      "sourceCount": 1.0,
      "pages_processed": 0.0,
      "records_seen": 0.0,
      "records_upserted": 0.0,
      "review_items_created": 1.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 6.329,
      "jobs_per_minute": 0.0,
      "run_ids": [
        185
      ],
      "enrichment": {
        "processed": 0,
        "requeued": 50,
        "failed_terminal": 0,
        "skipped_provider_degraded": 0
      },
      "coverage": {
        "chapters": 0,
        "any_contact": 0,
        "website": 0,
        "email": 0,
        "instagram": 0,
        "all_three": 0,
        "any_contact_rate": 0.0,
        "website_rate": 0.0,
        "email_rate": 0.0,
        "instagram_rate": 0.0,
        "all_three_rate": 0.0
      }
    },
    "kpis": {
      "legacyRecordsPerPage": 0.0,
      "adaptiveRecordsPerPage": 0.0,
      "recordsPerPageDelta": 0.0,
      "legacyPagesPerRecord": 0.0,
      "adaptivePagesPerRecord": 0.0,
      "pagesPerRecordDelta": 0.0,
      "legacyUpsertRatio": 0.0,
      "adaptiveUpsertRatio": 0.0,
      "upsertRatioDelta": 0.0,
      "legacyJobsPerMinute": 0.0,
      "adaptiveJobsPerMinute": 0.0,
      "jobsPerMinuteDelta": 0.0,
      "legacyReviewRate": 1.0,
      "adaptiveReviewRate": 1.0,
      "reviewRateDelta": 0.0,
      "legacyAnyContactRate": 0.0,
      "adaptiveAnyContactRate": 0.0,
      "anyContactRateDelta": 0.0,
      "legacyWebsiteRate": 0.0,
      "adaptiveWebsiteRate": 0.0,
      "websiteRateDelta": 0.0,
      "legacyEmailRate": 0.0,
      "adaptiveEmailRate": 0.0,
      "emailRateDelta": 0.0,
      "legacyInstagramRate": 0.0,
      "adaptiveInstagramRate": 0.0,
      "instagramRateDelta": 0.0,
      "legacyAllThreeRate": 0.0,
      "adaptiveAllThreeRate": 0.0,
      "allThreeRateDelta": 0.0,
      "balancedScore": 0.0
    },
    "slopes": {
      "recordsPerPageDeltaSlope": 0.0,
      "pagesPerRecordDeltaSlope": 0.0,
      "upsertRatioDeltaSlope": 0.0,
      "jobsPerMinuteDeltaSlope": 0.0,
      "reviewRateDeltaSlope": 0.0,
      "anyContactRateDeltaSlope": 0.0,
      "balancedScoreSlope": 0.0
    }
  }
]
```
