# Adaptive Train/Eval Epoch Report (2026-04-01 23:11:27 UTC)

- Epochs: `3`
- Adaptive runtime: `adaptive_assisted`
- Train sources: `sigma-chi-main, chi-psi-main, kappa-delta-rho-main`
- Eval sources: `delta-kappa-epsilon-main, alpha-tau-omega-main, delta-sigma-phi-main`

## KPI Delta Slope (Adaptive - Legacy)
- recordsPerPageDeltaSlope: `0.0`
- pagesPerRecordDeltaSlope: `0.0`
- upsertRatioDeltaSlope: `0.0`
- jobsPerMinuteDeltaSlope: `11.12775`
- reviewRateDeltaSlope: `0.0`
- balancedScoreSlope: `0.22255`

## Per-Epoch KPI Deltas
| Epoch | Records/Page Delta | Pages/Record Delta | Upsert Ratio Delta | Jobs/Min Delta | Review Rate Delta | Balanced Score |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | -60.1333 | 0.0335 | 0.0 | 173.1384 | 0.0 | 3.4561 |
| 2 | -60.1333 | 0.0335 | 0.0 | 195.999 | 0.0 | 3.9133 |
| 3 | -60.1333 | 0.0335 | 0.0 | 195.3939 | 0.0 | 3.9012 |

## Raw Rows
```json
[
  {
    "epoch": 1,
    "policyVersion": "adaptive-v1",
    "runtimeMode": "adaptive_assisted",
    "train": {
      "sourceCount": 3.0,
      "pages_processed": 13.0,
      "records_seen": 51.0,
      "records_upserted": 51.0,
      "review_items_created": 0.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 26.549,
      "jobs_per_minute": 115.2568
    },
    "replay": {
      "eventsApplied": 16,
      "windowDays": 7,
      "batchSize": 500,
      "runtimeMode": "adaptive_assisted",
      "sourceCount": 3
    },
    "evalLegacy": {
      "sourceCount": 3.0,
      "pages_processed": 4.0,
      "records_seen": 328.0,
      "records_upserted": 328.0,
      "review_items_created": 0.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 81.578,
      "jobs_per_minute": 241.2417
    },
    "evalAdaptive": {
      "sourceCount": 3.0,
      "pages_processed": 15.0,
      "records_seen": 328.0,
      "records_upserted": 328.0,
      "review_items_created": 0.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 47.493,
      "jobs_per_minute": 414.3801
    },
    "kpis": {
      "legacyRecordsPerPage": 82.0,
      "adaptiveRecordsPerPage": 21.8667,
      "recordsPerPageDelta": -60.1333,
      "legacyPagesPerRecord": 0.0122,
      "adaptivePagesPerRecord": 0.0457,
      "pagesPerRecordDelta": 0.0335,
      "legacyUpsertRatio": 1.0,
      "adaptiveUpsertRatio": 1.0,
      "upsertRatioDelta": 0.0,
      "legacyJobsPerMinute": 241.2417,
      "adaptiveJobsPerMinute": 414.3801,
      "jobsPerMinuteDelta": 173.1384,
      "legacyReviewRate": 0.0,
      "adaptiveReviewRate": 0.0,
      "reviewRateDelta": 0.0,
      "balancedScore": 3.4561
    },
    "slopes": {
      "recordsPerPageDeltaSlope": 0.0,
      "pagesPerRecordDeltaSlope": 0.0,
      "upsertRatioDeltaSlope": 0.0,
      "jobsPerMinuteDeltaSlope": 0.0,
      "reviewRateDeltaSlope": 0.0,
      "balancedScoreSlope": 0.0
    }
  },
  {
    "epoch": 2,
    "policyVersion": "adaptive-v1",
    "runtimeMode": "adaptive_assisted",
    "train": {
      "sourceCount": 3.0,
      "pages_processed": 14.0,
      "records_seen": 138.0,
      "records_upserted": 138.0,
      "review_items_created": 0.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 33.462,
      "jobs_per_minute": 247.4422
    },
    "replay": {
      "eventsApplied": 33,
      "windowDays": 7,
      "batchSize": 500,
      "runtimeMode": "adaptive_assisted",
      "sourceCount": 3
    },
    "evalLegacy": {
      "sourceCount": 3.0,
      "pages_processed": 4.0,
      "records_seen": 328.0,
      "records_upserted": 328.0,
      "review_items_created": 0.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 82.169,
      "jobs_per_minute": 239.5061
    },
    "evalAdaptive": {
      "sourceCount": 3.0,
      "pages_processed": 15.0,
      "records_seen": 328.0,
      "records_upserted": 328.0,
      "review_items_created": 0.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 45.189,
      "jobs_per_minute": 435.5051
    },
    "kpis": {
      "legacyRecordsPerPage": 82.0,
      "adaptiveRecordsPerPage": 21.8667,
      "recordsPerPageDelta": -60.1333,
      "legacyPagesPerRecord": 0.0122,
      "adaptivePagesPerRecord": 0.0457,
      "pagesPerRecordDelta": 0.0335,
      "legacyUpsertRatio": 1.0,
      "adaptiveUpsertRatio": 1.0,
      "upsertRatioDelta": 0.0,
      "legacyJobsPerMinute": 239.5061,
      "adaptiveJobsPerMinute": 435.5051,
      "jobsPerMinuteDelta": 195.999,
      "legacyReviewRate": 0.0,
      "adaptiveReviewRate": 0.0,
      "reviewRateDelta": 0.0,
      "balancedScore": 3.9133
    },
    "slopes": {
      "recordsPerPageDeltaSlope": 0.0,
      "pagesPerRecordDeltaSlope": 0.0,
      "upsertRatioDeltaSlope": 0.0,
      "jobsPerMinuteDeltaSlope": 22.8606,
      "reviewRateDeltaSlope": 0.0,
      "balancedScoreSlope": 0.4572
    }
  },
  {
    "epoch": 3,
    "policyVersion": "adaptive-v1",
    "runtimeMode": "adaptive_assisted",
    "train": {
      "sourceCount": 3.0,
      "pages_processed": 13.0,
      "records_seen": 51.0,
      "records_upserted": 51.0,
      "review_items_created": 0.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 33.69,
      "jobs_per_minute": 90.8268
    },
    "replay": {
      "eventsApplied": 49,
      "windowDays": 7,
      "batchSize": 500,
      "runtimeMode": "adaptive_assisted",
      "sourceCount": 3
    },
    "evalLegacy": {
      "sourceCount": 3.0,
      "pages_processed": 4.0,
      "records_seen": 328.0,
      "records_upserted": 328.0,
      "review_items_created": 0.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 79.327,
      "jobs_per_minute": 248.087
    },
    "evalAdaptive": {
      "sourceCount": 3.0,
      "pages_processed": 15.0,
      "records_seen": 328.0,
      "records_upserted": 328.0,
      "review_items_created": 0.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 44.376,
      "jobs_per_minute": 443.4809
    },
    "kpis": {
      "legacyRecordsPerPage": 82.0,
      "adaptiveRecordsPerPage": 21.8667,
      "recordsPerPageDelta": -60.1333,
      "legacyPagesPerRecord": 0.0122,
      "adaptivePagesPerRecord": 0.0457,
      "pagesPerRecordDelta": 0.0335,
      "legacyUpsertRatio": 1.0,
      "adaptiveUpsertRatio": 1.0,
      "upsertRatioDelta": 0.0,
      "legacyJobsPerMinute": 248.087,
      "adaptiveJobsPerMinute": 443.4809,
      "jobsPerMinuteDelta": 195.3939,
      "legacyReviewRate": 0.0,
      "adaptiveReviewRate": 0.0,
      "reviewRateDelta": 0.0,
      "balancedScore": 3.9012
    },
    "slopes": {
      "recordsPerPageDeltaSlope": 0.0,
      "pagesPerRecordDeltaSlope": 0.0,
      "upsertRatioDeltaSlope": 0.0,
      "jobsPerMinuteDeltaSlope": 11.12775,
      "reviewRateDeltaSlope": 0.0,
      "balancedScoreSlope": 0.22255
    }
  }
]
```
