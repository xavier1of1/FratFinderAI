# Integration Tests

This folder is reserved for cross-service integration tests (web + crawler + Postgres).

Planned scenarios:
- full crawl run writes chapters/provenance/review_items/field_jobs.
- dashboard API routes return DB-backed crawl outcomes.
- repeat crawl run remains idempotent and does not duplicate canonical chapter rows.