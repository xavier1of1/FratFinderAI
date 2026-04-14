from types import SimpleNamespace

from fratfinder_crawler.field_jobs import FieldJobResult
from fratfinder_crawler.models import FieldJob
from fratfinder_crawler.orchestration.field_job_graph import FieldJobGraphRuntime


def _job() -> FieldJob:
    return FieldJob(
        id="job-1",
        chapter_id="chapter-1",
        chapter_slug="chapter-1",
        chapter_name="Alpha Test",
        field_name="find_website",
        payload={"sourceSlug": "alpha-main"},
        attempts=1,
        max_attempts=3,
        claim_token="claim-1",
        source_base_url="https://example.org",
        website_url=None,
        instagram_url=None,
        contact_email=None,
        fraternity_slug="alpha-beta",
        source_id="source-1",
        source_slug="alpha-main",
        university_name="Example University",
        crawl_run_id=1,
        field_states={},
    )


class _FakeRepository:
    def __init__(self, jobs: list[FieldJob]):
        self.jobs = list(jobs)
        self.finished_runs: list[dict[str, object]] = []
        self.claim_args: list[dict[str, object]] = []

    def start_field_job_graph_run(self, **kwargs):
        _ = kwargs
        return 1

    def finish_field_job_graph_run(self, run_id, *, status, summary, error_message=None):
        self.finished_runs.append(
            {
                "run_id": run_id,
                "status": status,
                "summary": summary,
                "error_message": error_message,
            }
        )

    def append_field_job_graph_event(self, **kwargs):
        _ = kwargs

    def upsert_field_job_graph_checkpoint(self, **kwargs):
        _ = kwargs

    def insert_field_job_graph_decision(self, **kwargs):
        _ = kwargs

    def claim_next_field_job(self, worker_id, source_slug=None, field_name=None, require_confident_website_for_email=False):
        self.claim_args.append(
            {
                "worker_id": worker_id,
                "source_slug": source_slug,
                "field_name": field_name,
                "require_confident_website_for_email": require_confident_website_for_email,
            }
        )
        return self.jobs.pop(0) if self.jobs else None

    def create_field_job_review_item(self, job, review_item):
        _ = job, review_item

    def complete_field_job(self, job, chapter_updates, completed_payload, field_state_updates, provenance_records):
        _ = job, chapter_updates, completed_payload, field_state_updates, provenance_records

    def requeue_field_job(self, job, error, delay_seconds, preserve_attempt=False, payload_patch=None):
        _ = job, error, delay_seconds, preserve_attempt, payload_patch

    def fail_field_job_terminal(self, job, error):
        _ = job, error


def test_field_job_graph_runtime_marks_no_business_progress_when_no_job_claimed():
    repository = _FakeRepository([])
    engine = SimpleNamespace(process_claimed_job=lambda job: None, _base_backoff_seconds=30)
    runtime = FieldJobGraphRuntime(
        repository=repository,
        engine=engine,
        worker_id="worker-1",
        runtime_mode="langgraph_primary",
        graph_durability="sync",
        source_slug=None,
        field_name=None,
    )

    result = runtime.process(limit=1)

    assert result == {"processed": 0, "requeued": 0, "failed_terminal": 0}
    assert repository.finished_runs[0]["status"] == "succeeded"
    assert repository.finished_runs[0]["summary"]["businessStatus"] == "no_business_progress"
    assert repository.finished_runs[0]["summary"]["businessProgressCount"] == 0


def test_field_job_graph_runtime_marks_progressed_when_job_completes():
    repository = _FakeRepository([_job()])
    engine = SimpleNamespace(
        process_claimed_job=lambda job: FieldJobResult(
            chapter_updates={"website_url": "https://example.org/chapter"},
            completed_payload={"status": "updated", "website_url": "https://example.org/chapter"},
            field_state_updates={"website_url": "found"},
        ),
        _base_backoff_seconds=30,
    )
    runtime = FieldJobGraphRuntime(
        repository=repository,
        engine=engine,
        worker_id="worker-1",
        runtime_mode="langgraph_primary",
        graph_durability="sync",
        source_slug=None,
        field_name=None,
    )

    result = runtime.process(limit=1)

    assert result == {"processed": 1, "requeued": 0, "failed_terminal": 0}
    assert repository.finished_runs[0]["status"] == "succeeded"
    assert repository.finished_runs[0]["summary"]["businessStatus"] == "progressed"
    assert repository.finished_runs[0]["summary"]["businessProgressCount"] == 1


def test_field_job_graph_runtime_defaults_email_confidence_gate_to_false_when_engine_lacks_flag():
    repository = _FakeRepository([_job()])
    engine = SimpleNamespace(
        process_claimed_job=lambda job: FieldJobResult(
            chapter_updates={"website_url": "https://example.org/chapter"},
            completed_payload={"status": "updated", "website_url": "https://example.org/chapter"},
            field_state_updates={"website_url": "found"},
        ),
        _base_backoff_seconds=30,
    )
    runtime = FieldJobGraphRuntime(
        repository=repository,
        engine=engine,
        worker_id="worker-1",
        runtime_mode="langgraph_primary",
        graph_durability="sync",
        source_slug=None,
        field_name=None,
    )

    result = runtime.process(limit=1)

    assert result["processed"] == 1
    assert repository.claim_args[0]["require_confident_website_for_email"] is False
