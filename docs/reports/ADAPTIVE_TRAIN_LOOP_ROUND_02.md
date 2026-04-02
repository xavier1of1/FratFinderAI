# Adaptive Train/Eval Epoch Report (2026-04-01 23:18:17 UTC)

- Epochs: `3`
- Adaptive runtime: `adaptive_assisted`
- Train sources: `sigma-chi-main, chi-psi-main, kappa-delta-rho-main`
- Eval sources: `delta-kappa-epsilon-main, alpha-tau-omega-main, delta-sigma-phi-main`

## KPI Delta Slope (Adaptive - Legacy)
- recordsPerPageDeltaSlope: `0.0`
- pagesPerRecordDeltaSlope: `0.0`
- upsertRatioDeltaSlope: `0.0`
- jobsPerMinuteDeltaSlope: `-11.75905`
- reviewRateDeltaSlope: `0.0`
- balancedScoreSlope: `-0.23515`

## Per-Epoch KPI Deltas
| Epoch | Records/Page Delta | Pages/Record Delta | Upsert Ratio Delta | Jobs/Min Delta | Review Rate Delta | Balanced Score |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | -60.1333 | 0.0335 | 0.0 | 203.0012 | 0.0 | 4.0533 |
| 2 | -60.1333 | 0.0335 | 0.0 | 169.3999 | 0.0 | 3.3813 |
| 3 | -60.1333 | 0.0335 | 0.0 | 179.4831 | 0.0 | 3.583 |

## Raw Rows
```json
[
  {
    "epoch": 1,
    "policyVersion": "adaptive-v1",
    "runtimeMode": "adaptive_assisted",
    "train": {
      "sourceCount": 3.0,
      "pages_processed": 14.0,
      "records_seen": 138.0,
      "records_upserted": 138.0,
      "review_items_created": 0.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 27.395,
      "jobs_per_minute": 302.2477
    },
    "replay": {
      "eventsApplied": 66,
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
      "elapsed_seconds": 71.582,
      "jobs_per_minute": 274.9285
    },
    "evalAdaptive": {
      "sourceCount": 3.0,
      "pages_processed": 15.0,
      "records_seen": 328.0,
      "records_upserted": 328.0,
      "review_items_created": 0.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 41.178,
      "jobs_per_minute": 477.9297
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
      "legacyJobsPerMinute": 274.9285,
      "adaptiveJobsPerMinute": 477.9297,
      "jobsPerMinuteDelta": 203.0012,
      "legacyReviewRate": 0.0,
      "adaptiveReviewRate": 0.0,
      "reviewRateDelta": 0.0,
      "balancedScore": 4.0533
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
      "elapsed_seconds": 27.712,
      "jobs_per_minute": 298.7828
    },
    "replay": {
      "eventsApplied": 83,
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
      "elapsed_seconds": 72.815,
      "jobs_per_minute": 270.2743
    },
    "evalAdaptive": {
      "sourceCount": 3.0,
      "pages_processed": 15.0,
      "records_seen": 328.0,
      "records_upserted": 328.0,
      "review_items_created": 0.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 44.76,
      "jobs_per_minute": 439.6742
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
      "legacyJobsPerMinute": 270.2743,
      "adaptiveJobsPerMinute": 439.6742,
      "jobsPerMinuteDelta": 169.3999,
      "legacyReviewRate": 0.0,
      "adaptiveReviewRate": 0.0,
      "reviewRateDelta": 0.0,
      "balancedScore": 3.3813
    },
    "slopes": {
      "recordsPerPageDeltaSlope": 0.0,
      "pagesPerRecordDeltaSlope": 0.0,
      "upsertRatioDeltaSlope": 0.0,
      "jobsPerMinuteDeltaSlope": -33.6013,
      "reviewRateDeltaSlope": 0.0,
      "balancedScoreSlope": -0.672
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
      "elapsed_seconds": 20.241,
      "jobs_per_minute": 151.1786
    },
    "replay": {
      "eventsApplied": 99,
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
      "elapsed_seconds": 63.827,
      "jobs_per_minute": 308.3318
    },
    "evalAdaptive": {
      "sourceCount": 3.0,
      "pages_processed": 15.0,
      "records_seen": 328.0,
      "records_upserted": 328.0,
      "review_items_created": 0.0,
      "field_jobs_created": 0.0,
      "elapsed_seconds": 40.343,
      "jobs_per_minute": 487.8149
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
      "legacyJobsPerMinute": 308.3318,
      "adaptiveJobsPerMinute": 487.8149,
      "jobsPerMinuteDelta": 179.4831,
      "legacyReviewRate": 0.0,
      "adaptiveReviewRate": 0.0,
      "reviewRateDelta": 0.0,
      "balancedScore": 3.583
    },
    "slopes": {
      "recordsPerPageDeltaSlope": 0.0,
      "pagesPerRecordDeltaSlope": 0.0,
      "upsertRatioDeltaSlope": 0.0,
      "jobsPerMinuteDeltaSlope": -11.75905,
      "reviewRateDeltaSlope": 0.0,
      "balancedScoreSlope": -0.23515
    }
  }
]
```
