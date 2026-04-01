from __future__ import annotations

import logging
from dataclasses import asdict, is_dataclass
from typing import Any, Callable

from langgraph.graph import END, StateGraph

from fratfinder_crawler.analysis import (
    analyze_page,
    classify_source,
    detect_embedded_data,
    select_extraction_plan,
)
from fratfinder_crawler.adapters.registry import AdapterRegistry
from fratfinder_crawler.candidate_sanitizer import sanitize_as_email, sanitize_as_instagram, sanitize_as_website
from fratfinder_crawler.config import get_settings
from fratfinder_crawler.db.repository import CrawlerRepository
from fratfinder_crawler.http.client import HttpClient
from fratfinder_crawler.llm.classifier import classify_source_with_llm
from fratfinder_crawler.llm.client import LLMUnavailableError
from fratfinder_crawler.llm.extractor import ExtractionValidationError, extract_records_with_metadata
from fratfinder_crawler.logging_utils import log_event
from fratfinder_crawler.models import AmbiguousRecordError, CrawlMetrics, ExtractedChapter, ReviewItemCandidate
from fratfinder_crawler.normalization import normalize_record
from fratfinder_crawler.orchestration.state import CrawlGraphState
from fratfinder_crawler.orchestration.navigation import (
    detect_chapter_index_mode,
    extract_chapter_stubs,
    extract_contacts_from_chapter_site,
    follow_chapter_detail_or_outbound,
)

LOGGER = logging.getLogger(__name__)


class CrawlOrchestrator:
    def __init__(self, repository: CrawlerRepository, http_client: HttpClient, registry: AdapterRegistry):
        self._repository = repository
        self._http = http_client
        self._registry = registry
        self._graph = self._build_graph()

    def run_for_source(self, source) -> CrawlMetrics:
        run_id = self._repository.start_crawl_run(source.id)
        log_event(LOGGER, "crawl_run_started", run_id=run_id, source_slug=source.source_slug)
        initial_state: CrawlGraphState = {
            "source": source,
            "run_id": run_id,
            "review_items": [],
            "metrics": CrawlMetrics(),
            "final_status": "succeeded",
            "strategy_attempts": 0,
            "llm_calls_used": 0,
        }
        final_state = self._graph.invoke(initial_state)
        log_event(
            LOGGER,
            "crawl_run_finished",
            run_id=run_id,
            source_slug=source.source_slug,
            records_upserted=final_state["metrics"].records_upserted,
            review_items_created=final_state["metrics"].review_items_created,
            field_jobs_created=final_state["metrics"].field_jobs_created,
            llm_calls_used=final_state.get("llm_calls_used", 0),
        )
        return final_state["metrics"]

    def _with_error_boundary(self, func: Callable[[CrawlGraphState], dict]) -> Callable[[CrawlGraphState], dict]:
        def wrapper(state: CrawlGraphState) -> dict:
            try:
                return func(state)
            except Exception as exc:  # pragma: no cover - guardrail path
                log_event(
                    LOGGER,
                    "graph_node_failed",
                    level=logging.ERROR,
                    error=str(exc),
                    node=func.__name__,
                    run_id=state.get("run_id"),
                )
                return {"error": str(exc)}

        return wrapper

    def _build_graph(self):
        graph = StateGraph(CrawlGraphState)

        graph.add_node("fetch_page", self._with_error_boundary(self._fetch_page))
        graph.add_node("analyze_page_structure", self._with_error_boundary(self._analyze_page_structure))
        graph.add_node("classify_source_type", self._with_error_boundary(self._classify_source_type))
        graph.add_node("detect_embedded_data", self._with_error_boundary(self._detect_embedded_data))
        graph.add_node("detect_chapter_index_mode", self._with_error_boundary(self._detect_chapter_index_mode))
        graph.add_node("extract_chapter_stubs", self._with_error_boundary(self._extract_chapter_stubs))
        graph.add_node("follow_chapter_detail_or_outbound", self._with_error_boundary(self._follow_chapter_detail_or_outbound))
        graph.add_node("extract_contacts_from_chapter_site", self._with_error_boundary(self._extract_contacts_from_chapter_site))
        graph.add_node("choose_extraction_strategy", self._with_error_boundary(self._choose_extraction_strategy))
        graph.add_node("extract_records", self._with_error_boundary(self._extract_records))
        graph.add_node("validate_records", self._with_error_boundary(self._validate_records))
        graph.add_node("normalize_records", self._with_error_boundary(self._normalize_records))
        graph.add_node("persist_records", self._with_error_boundary(self._persist_records))
        graph.add_node("spawn_followup_jobs", self._with_error_boundary(self._spawn_followup_jobs))
        graph.add_node("finalize", self._with_error_boundary(self._finalize))

        graph.set_entry_point("fetch_page")

        graph.add_conditional_edges("fetch_page", self._has_error, {"ok": "analyze_page_structure", "error": "finalize"})
        graph.add_conditional_edges("analyze_page_structure", self._has_error, {"ok": "classify_source_type", "error": "finalize"})
        graph.add_conditional_edges("classify_source_type", self._has_error, {"ok": "detect_embedded_data", "error": "finalize"})
        graph.add_conditional_edges("detect_embedded_data", self._has_error, {"ok": "detect_chapter_index_mode", "error": "finalize"})
        graph.add_conditional_edges("detect_chapter_index_mode", self._has_error, {"ok": "extract_chapter_stubs", "error": "finalize"})
        graph.add_conditional_edges("extract_chapter_stubs", self._has_error, {"ok": "follow_chapter_detail_or_outbound", "error": "finalize"})
        graph.add_conditional_edges("follow_chapter_detail_or_outbound", self._has_error, {"ok": "extract_contacts_from_chapter_site", "error": "finalize"})
        graph.add_conditional_edges("extract_contacts_from_chapter_site", self._has_error, {"ok": "choose_extraction_strategy", "error": "finalize"})
        graph.add_conditional_edges("choose_extraction_strategy", self._has_error, {"ok": "extract_records", "error": "finalize"})
        graph.add_conditional_edges("extract_records", self._has_error, {"ok": "validate_records", "error": "finalize"})
        graph.add_conditional_edges("validate_records", self._has_error, {"ok": "normalize_records", "error": "finalize"})
        graph.add_conditional_edges("normalize_records", self._has_error, {"ok": "persist_records", "error": "finalize"})
        graph.add_conditional_edges("persist_records", self._has_error, {"ok": "spawn_followup_jobs", "error": "finalize"})
        graph.add_edge("spawn_followup_jobs", "finalize")
        graph.add_edge("finalize", END)

        return graph.compile()

    def _has_error(self, state: CrawlGraphState) -> str:
        return "error" if state.get("error") else "ok"

    def _fetch_page(self, state: CrawlGraphState) -> dict:
        source = state["source"]
        run_id = state["run_id"]
        html = self._http.get(source.list_url)

        metrics = state["metrics"]
        metrics.pages_processed += 1
        log_event(LOGGER, "page_fetched", run_id=run_id, source_slug=source.source_slug, source_url=source.list_url)

        return {"html": html, "metrics": metrics}

    def _analyze_page_structure(self, state: CrawlGraphState) -> dict:
        analysis = analyze_page(state["html"])
        log_event(
            LOGGER,
            "page_analyzed",
            run_id=state["run_id"],
            source_slug=state["source"].source_slug,
            probable_page_role=analysis.probable_page_role,
            table_count=analysis.table_count,
            repeated_block_count=analysis.repeated_block_count,
        )
        return {"page_analysis": analysis}

    def _classify_source_type(self, state: CrawlGraphState) -> dict:
        settings = get_settings()
        heuristic_classification = classify_source(state["page_analysis"], llm_enabled=False)
        classification = heuristic_classification
        llm_calls_used = state.get("llm_calls_used", 0)
        decision_reason = "heuristic"

        if heuristic_classification.confidence < 0.5 and settings.crawler_llm_enabled:
            embedded_preview = detect_embedded_data(state["html"], state["source"].list_url)
            if embedded_preview.found:
                decision_reason = "embedded_data_detected_pre_classification"
            elif llm_calls_used >= settings.crawler_llm_max_calls_per_run:
                decision_reason = "llm_budget_exhausted"
            else:
                llm_calls_used += 1
                try:
                    classification = classify_source_with_llm(state["page_analysis"])
                    decision_reason = "llm_fallback"
                except LLMUnavailableError as exc:
                    decision_reason = "llm_unavailable"
                    log_event(
                        LOGGER,
                        "source_classification_llm_skipped",
                        run_id=state["run_id"],
                        source_slug=state["source"].source_slug,
                        error=str(exc),
                    )
                except Exception as exc:
                    decision_reason = "llm_failed"
                    log_event(
                        LOGGER,
                        "source_classification_llm_failed",
                        level=logging.WARNING,
                        run_id=state["run_id"],
                        source_slug=state["source"].source_slug,
                        error=str(exc),
                    )

        log_event(
            LOGGER,
            "source_classified",
            run_id=state["run_id"],
            source_slug=state["source"].source_slug,
            page_type=classification.page_type,
            confidence=classification.confidence,
            strategy=classification.recommended_strategy,
            classified_by=classification.classified_by,
            decision_reason=decision_reason,
            llm_calls_used=llm_calls_used,
        )
        return {"classification": classification, "llm_calls_used": llm_calls_used}

    def _detect_embedded_data(self, state: CrawlGraphState) -> dict:
        embedded_data = detect_embedded_data(state["html"], state["source"].list_url)
        log_event(
            LOGGER,
            "embedded_data_detected",
            run_id=state["run_id"],
            source_slug=state["source"].source_slug,
            found=embedded_data.found,
            data_type=embedded_data.data_type,
            api_url=embedded_data.api_url,
        )
        return {"embedded_data": embedded_data}

    def _detect_chapter_index_mode(self, state: CrawlGraphState) -> dict:
        mode, confidence, reason = detect_chapter_index_mode(
            state["html"],
            state["page_analysis"],
            state["classification"],
            state["embedded_data"],
            state["source"].metadata,
        )
        log_event(
            LOGGER,
            "chapter_index_mode_detected",
            run_id=state["run_id"],
            source_slug=state["source"].source_slug,
            mode=mode,
            confidence=confidence,
            reason=reason,
        )
        return {
            "chapter_index_mode": mode,
            "chapter_index_mode_confidence": confidence,
            "chapter_index_mode_reason": reason,
        }

    def _extract_chapter_stubs(self, state: CrawlGraphState) -> dict:
        stubs = extract_chapter_stubs(
            registry=self._registry,
            html=state["html"],
            source_url=state["source"].list_url,
            mode=state.get("chapter_index_mode", "mixed"),
            embedded_data=state["embedded_data"],
            http_client=self._http,
            source_metadata=state["source"].metadata,
        )
        log_event(
            LOGGER,
            "chapter_stubs_extracted",
            run_id=state["run_id"],
            source_slug=state["source"].source_slug,
            mode=state.get("chapter_index_mode", "mixed"),
            stubs=len(stubs),
        )
        return {"chapter_stubs": stubs}

    def _follow_chapter_detail_or_outbound(self, state: CrawlGraphState) -> dict:
        settings = get_settings()
        stubs = state.get("chapter_stubs", [])
        pages_by_stub, nav_stats = follow_chapter_detail_or_outbound(
            stubs=stubs,
            source_url=state["source"].list_url,
            http_client=self._http,
            max_hops_per_stub=settings.crawler_navigation_max_hops_per_stub,
            max_pages_per_run=settings.crawler_navigation_max_pages_per_run,
        )
        metrics = state["metrics"]
        metrics.pages_processed += nav_stats.get("fetched_pages", 0)
        log_event(
            LOGGER,
            "chapter_stub_follow_completed",
            run_id=state["run_id"],
            source_slug=state["source"].source_slug,
            fetched_pages=nav_stats.get("fetched_pages", 0),
            skipped_by_domain=nav_stats.get("skipped_by_domain", 0),
            errors=nav_stats.get("errors", 0),
        )
        return {"chapter_follow_pages": pages_by_stub, "navigation_stats": nav_stats, "metrics": metrics}

    def _extract_contacts_from_chapter_site(self, state: CrawlGraphState) -> dict:
        contact_hints = extract_contacts_from_chapter_site(
            state.get("chapter_stubs", []),
            state.get("chapter_follow_pages", {}),
        )
        log_event(
            LOGGER,
            "chapter_contact_hints_extracted",
            run_id=state["run_id"],
            source_slug=state["source"].source_slug,
            hinted_chapters=len(contact_hints),
        )
        return {"chapter_contact_hints": contact_hints}

    def _choose_extraction_strategy(self, state: CrawlGraphState) -> dict:
        settings = get_settings()
        extraction_plan = select_extraction_plan(
            page_analysis=state["page_analysis"],
            classification=state["classification"],
            embedded_data=state["embedded_data"],
            llm_enabled=settings.crawler_llm_enabled,
            source_metadata=state["source"].metadata,
        )
        strategy_attempts = state.get("strategy_attempts", 0) + 1
        log_event(
            LOGGER,
            "extraction_strategy_chosen",
            run_id=state["run_id"],
            source_slug=state["source"].source_slug,
            primary_strategy=extraction_plan.primary_strategy,
            fallback_strategies=extraction_plan.fallback_strategies,
            llm_allowed=extraction_plan.llm_allowed,
            llm_calls_used=state.get("llm_calls_used", 0),
            source_hint_applied=extraction_plan.source_hint_applied,
        )
        return {"extraction_plan": extraction_plan, "strategy_attempts": strategy_attempts}

    def _extract_records(self, state: CrawlGraphState) -> dict:
        source = state["source"]
        run_id = state["run_id"]
        plan = state["extraction_plan"]
        review_items = list(state.get("review_items", []))
        metrics = state["metrics"]
        llm_calls_used = state.get("llm_calls_used", 0)
        extraction_notes: str | None = None
        page_level_confidence: float | None = None
        strategy_used = plan.primary_strategy
        stub_records = self._build_stub_records(state)

        if plan.primary_strategy == "review":
            if stub_records:
                strategy_used = "chapter_stub_navigation"
                metrics.records_seen += len(stub_records)
                log_event(
                    LOGGER,
                    "records_extracted_from_stubs",
                    run_id=run_id,
                    source_slug=source.source_slug,
                    records_seen=len(stub_records),
                    strategy=strategy_used,
                )
                return {
                    "review_items": review_items,
                    "extracted": stub_records,
                    "final_status": state.get("final_status", "succeeded"),
                    "metrics": metrics,
                    "llm_calls_used": llm_calls_used,
                    "page_level_confidence": page_level_confidence,
                    "extraction_notes": extraction_notes,
                    "strategy_used": strategy_used,
                }
            review_items.append(
                ReviewItemCandidate(
                    item_type="unsupported_or_unclear_source",
                    reason="Unable to determine a supported extraction strategy for this source page",
                    source_slug=source.source_slug,
                    chapter_slug=None,
                    payload={
                        "pageType": state["classification"].page_type,
                        "recommendedStrategy": plan.primary_strategy,
                        "probablePageRole": state["page_analysis"].probable_page_role,
                    },
                )
            )
            return {
                "review_items": review_items,
                "extracted": [],
                "final_status": "partial",
                "metrics": metrics,
                "llm_calls_used": llm_calls_used,
                "page_level_confidence": page_level_confidence,
                "extraction_notes": extraction_notes,
            }

        if plan.primary_strategy == "llm":
            settings = get_settings()
            if state.get("embedded_data") and state["embedded_data"].found:
                review_items.append(
                    ReviewItemCandidate(
                        item_type="llm_blocked_embedded_data",
                        reason="LLM extraction is disabled for pages with embedded data",
                        source_slug=source.source_slug,
                        chapter_slug=None,
                        payload={"strategy": "llm", "dataType": state["embedded_data"].data_type},
                    )
                )
                return {
                    "review_items": review_items,
                    "extracted": stub_records,
                    "final_status": "partial",
                    "metrics": metrics,
                    "llm_calls_used": llm_calls_used,
                    "page_level_confidence": page_level_confidence,
                    "extraction_notes": extraction_notes,
                }

            if not settings.crawler_llm_enabled:
                review_items.append(
                    ReviewItemCandidate(
                        item_type="llm_disabled",
                        reason="LLM extraction is disabled for this crawl run",
                        source_slug=source.source_slug,
                        chapter_slug=None,
                        payload={"strategy": "llm"},
                    )
                )
                return {
                    "review_items": review_items,
                    "extracted": stub_records,
                    "final_status": "partial",
                    "metrics": metrics,
                    "llm_calls_used": llm_calls_used,
                    "page_level_confidence": page_level_confidence,
                    "extraction_notes": extraction_notes,
                }

            if llm_calls_used >= settings.crawler_llm_max_calls_per_run:
                review_items.append(
                    ReviewItemCandidate(
                        item_type="llm_budget_exhausted",
                        reason="LLM budget exhausted before extraction",
                        source_slug=source.source_slug,
                        chapter_slug=None,
                        payload={
                            "strategy": "llm",
                            "maxCallsPerRun": settings.crawler_llm_max_calls_per_run,
                        },
                    )
                )
                return {
                    "review_items": review_items,
                    "extracted": stub_records,
                    "final_status": "partial",
                    "metrics": metrics,
                    "llm_calls_used": llm_calls_used,
                    "page_level_confidence": page_level_confidence,
                    "extraction_notes": extraction_notes,
                }

            llm_calls_used += 1
            try:
                llm_result = extract_records_with_metadata(state["page_analysis"], source.list_url)
            except ExtractionValidationError as exc:
                review_items.append(
                    ReviewItemCandidate(
                        item_type="llm_extraction_invalid",
                        reason=str(exc),
                        source_slug=source.source_slug,
                        chapter_slug=None,
                        payload={"strategy": "llm"},
                    )
                )
                return {
                    "review_items": review_items,
                    "extracted": stub_records,
                    "final_status": "partial",
                    "metrics": metrics,
                    "llm_calls_used": llm_calls_used,
                    "page_level_confidence": page_level_confidence,
                    "extraction_notes": extraction_notes,
                }
            except LLMUnavailableError as exc:
                review_items.append(
                    ReviewItemCandidate(
                        item_type="llm_unavailable",
                        reason=str(exc),
                        source_slug=source.source_slug,
                        chapter_slug=None,
                        payload={"strategy": "llm"},
                    )
                )
                return {
                    "review_items": review_items,
                    "extracted": stub_records,
                    "final_status": "partial",
                    "metrics": metrics,
                    "llm_calls_used": llm_calls_used,
                    "page_level_confidence": page_level_confidence,
                    "extraction_notes": extraction_notes,
                }
            except Exception as exc:
                review_items.append(
                    ReviewItemCandidate(
                        item_type="llm_extraction_failed",
                        reason=str(exc),
                        source_slug=source.source_slug,
                        chapter_slug=None,
                        payload={"strategy": "llm"},
                    )
                )
                return {
                    "review_items": review_items,
                    "extracted": [],
                    "final_status": "partial",
                    "metrics": metrics,
                    "llm_calls_used": llm_calls_used,
                    "page_level_confidence": page_level_confidence,
                    "extraction_notes": extraction_notes,
                }

            extraction_notes = llm_result.extraction_notes
            page_level_confidence = llm_result.page_level_confidence
            if page_level_confidence < 0.5:
                review_items.append(
                    ReviewItemCandidate(
                        item_type="llm_low_page_confidence",
                        reason="LLM page confidence below persistence threshold",
                        source_slug=source.source_slug,
                        chapter_slug=None,
                        payload={
                            "strategy": "llm",
                            "pageLevelConfidence": page_level_confidence,
                            "extractionNotes": extraction_notes,
                        },
                    )
                )
                return {
                    "review_items": review_items,
                    "extracted": stub_records,
                    "final_status": "partial",
                    "metrics": metrics,
                    "llm_calls_used": llm_calls_used,
                    "page_level_confidence": page_level_confidence,
                    "extraction_notes": extraction_notes,
                }

            extracted = llm_result.records
        else:
            attempted_strategies: list[str] = []
            extracted = []
            strategy_sequence = [plan.primary_strategy] + [
                strategy
                for strategy in plan.fallback_strategies
                if strategy not in {"review", "llm"}
            ]
            for strategy_name in strategy_sequence:
                adapter = self._registry.get(strategy_name)
                if adapter is None:
                    continue
                attempted_strategies.append(strategy_name)
                candidate_records = adapter.parse(
                    state["html"],
                    source.list_url,
                    api_url=state["embedded_data"].api_url if state.get("embedded_data") else None,
                    http_client=self._http,
                    source_metadata=source.metadata,
                )
                if candidate_records:
                    extracted = candidate_records
                    strategy_used = strategy_name
                    break

            if not attempted_strategies:
                review_items.append(
                    ReviewItemCandidate(
                        item_type="unsupported_strategy",
                        reason=f"No adapter registered for strategy={plan.primary_strategy}",
                        source_slug=source.source_slug,
                        chapter_slug=None,
                        payload={
                            "strategy": plan.primary_strategy,
                            "fallbackStrategies": plan.fallback_strategies,
                        },
                    )
                )
                return {
                    "review_items": review_items,
                    "extracted": stub_records,
                    "final_status": "partial",
                    "metrics": metrics,
                    "llm_calls_used": llm_calls_used,
                    "page_level_confidence": page_level_confidence,
                    "extraction_notes": extraction_notes,
                }

        extracted = self._merge_extracted_records(extracted, stub_records)
        metrics.records_seen += len(extracted)
        log_event(
            LOGGER,
            "records_extracted",
            run_id=run_id,
            source_slug=source.source_slug,
            strategy=strategy_used,
            records_seen=len(extracted),
            llm_calls_used=llm_calls_used,
            page_level_confidence=page_level_confidence,
        )

        final_status = state.get("final_status", "succeeded")
        if not extracted:
            payload = {
                "strategy": strategy_used,
                "pageType": state["classification"].page_type,
            }
            if strategy_used != plan.primary_strategy:
                payload["initialStrategy"] = plan.primary_strategy
                payload["fallbackStrategies"] = plan.fallback_strategies
            if extraction_notes:
                payload["extractionNotes"] = extraction_notes
            if page_level_confidence is not None:
                payload["pageLevelConfidence"] = page_level_confidence
            review_items.append(
                ReviewItemCandidate(
                    item_type="empty_extraction",
                    reason=f"Strategy {strategy_used} returned no chapter records",
                    source_slug=source.source_slug,
                    chapter_slug=None,
                    payload=payload,
                )
            )
            final_status = "partial"

        return {
            "extracted": extracted,
            "metrics": metrics,
            "review_items": review_items,
            "final_status": final_status,
            "llm_calls_used": llm_calls_used,
            "page_level_confidence": page_level_confidence,
            "extraction_notes": extraction_notes,
            "strategy_used": strategy_used,
        }

    def _validate_records(self, state: CrawlGraphState) -> dict:
        source = state["source"]
        valid_records = []
        review_items = list(state.get("review_items", []))
        strategy = state.get("strategy_used") or (state.get("extraction_plan").primary_strategy if state.get("extraction_plan") else None)

        for record in state.get("extracted", []):
            if not record.name or not record.name.strip() or record.source_confidence <= 0.0:
                review_items.append(
                    ReviewItemCandidate(
                        item_type="invalid_record",
                        reason="Extracted record failed validation",
                        source_slug=source.source_slug,
                        chapter_slug=None,
                        payload={
                            "sourceUrl": record.source_url,
                            "sourceConfidence": record.source_confidence,
                        },
                    )
                )
                continue

            if strategy == "llm" and record.source_confidence < 0.60:
                review_items.append(
                    ReviewItemCandidate(
                        item_type="low_confidence_record",
                        reason="LLM extracted record fell below the minimum confidence threshold",
                        source_slug=source.source_slug,
                        chapter_slug=None,
                        payload={
                            "sourceUrl": record.source_url,
                            "sourceConfidence": record.source_confidence,
                            "recordName": record.name,
                        },
                    )
                )
                continue

            valid_records.append(record)

        final_status = state.get("final_status", "succeeded")
        if review_items and not valid_records:
            final_status = "partial"

        return {
            "extracted": valid_records,
            "review_items": review_items,
            "final_status": final_status,
        }

    def _normalize_records(self, state: CrawlGraphState) -> dict:
        source = state["source"]
        run_id = state["run_id"]
        review_items = list(state.get("review_items", []))
        normalized: list[dict] = []

        for record in state.get("extracted", []):
            try:
                chapter, provenance = normalize_record(source, record)
                normalized.append({"chapter": chapter, "provenance": provenance})
            except AmbiguousRecordError as exc:
                review_items.append(
                    ReviewItemCandidate(
                        item_type="ambiguous_record",
                        reason=str(exc),
                        source_slug=source.source_slug,
                        chapter_slug=None,
                        payload={"source_url": record.source_url, "snippet": record.source_snippet},
                    )
                )
                log_event(
                    LOGGER,
                    "record_ambiguous",
                    run_id=run_id,
                    source_slug=source.source_slug,
                    error=str(exc),
                )

        final_status = state.get("final_status", "succeeded")
        if review_items and not normalized:
            final_status = "partial"

        return {
            "normalized": normalized,
            "review_items": review_items,
            "final_status": final_status,
        }

    def _persist_records(self, state: CrawlGraphState) -> dict:
        source = state["source"]
        run_id = state["run_id"]
        metrics = state["metrics"]
        persisted: list[dict] = []

        for bundle in state.get("normalized", []):
            chapter = bundle["chapter"]
            provenance = bundle["provenance"]

            chapter_id = self._repository.upsert_chapter(source, chapter)
            self._repository.insert_provenance(chapter_id, source.id, run_id, provenance)
            metrics.records_upserted += 1
            persisted.append({"chapter": chapter, "provenance": provenance, "chapter_id": chapter_id})
            log_event(
                LOGGER,
                "chapter_upserted",
                run_id=run_id,
                source_slug=source.source_slug,
                chapter_slug=chapter.slug,
            )

        for review_item in state.get("review_items", []):
            self._repository.create_review_item(source.id, run_id, review_item)
            metrics.review_items_created += 1
            log_event(
                LOGGER,
                "review_item_created",
                run_id=run_id,
                source_slug=source.source_slug,
                item_type=review_item.item_type,
            )

        return {"metrics": metrics, "normalized": persisted}

    def _spawn_followup_jobs(self, state: CrawlGraphState) -> dict:
        source = state["source"]
        run_id = state["run_id"]
        metrics = state["metrics"]

        for bundle in state.get("normalized", []):
            chapter = bundle["chapter"]
            chapter_id = bundle["chapter_id"]
            if not chapter.missing_optional_fields:
                continue

            created = self._repository.create_field_jobs(
                chapter_id=chapter_id,
                crawl_run_id=run_id,
                chapter_slug=chapter.slug,
                source_slug=source.source_slug,
                missing_fields=chapter.missing_optional_fields,
            )
            metrics.field_jobs_created += created
            if created:
                log_event(
                    LOGGER,
                    "field_jobs_created",
                    run_id=run_id,
                    source_slug=source.source_slug,
                    chapter_slug=chapter.slug,
                    count=created,
                )

        return {"metrics": metrics}

    def _finalize(self, state: CrawlGraphState) -> dict:
        source = state["source"]
        run_id = state["run_id"]
        metrics = state.get("metrics", CrawlMetrics())
        page_analysis_payload = _to_serializable(state.get("page_analysis"))
        classification_payload = _to_serializable(state.get("classification"))
        extraction_metadata = {
            "strategy_used": state.get("strategy_used")
            or (state.get("extraction_plan").primary_strategy if state.get("extraction_plan") else None),
            "page_level_confidence": state.get("page_level_confidence"),
            "llm_calls_used": state.get("llm_calls_used", 0),
            "extraction_notes": state.get("extraction_notes"),
            "strategy_attempts": state.get("strategy_attempts", 0),
            "chapter_index_mode": state.get("chapter_index_mode"),
            "chapter_index_mode_confidence": state.get("chapter_index_mode_confidence"),
            "chapter_index_mode_reason": state.get("chapter_index_mode_reason"),
            "chapter_stub_count": len(state.get("chapter_stubs", [])),
            "navigation_stats": state.get("navigation_stats", {}),
        }

        if state.get("error"):
            self._repository.create_review_item(
                source_id=source.id,
                crawl_run_id=run_id,
                candidate=ReviewItemCandidate(
                    item_type="crawl_failure",
                    reason=state.get("error", "unknown failure"),
                    source_slug=source.source_slug,
                    chapter_slug=None,
                    payload={"source_url": source.list_url},
                ),
            )
            metrics.review_items_created += 1
            self._repository.finish_crawl_run(
                run_id=run_id,
                status="failed",
                metrics=metrics,
                last_error=state.get("error"),
                page_analysis=page_analysis_payload,
                classification=classification_payload,
                extraction_metadata=extraction_metadata,
            )
            log_event(
                LOGGER,
                "crawl_run_failed",
                level=logging.ERROR,
                run_id=run_id,
                source_slug=source.source_slug,
                error=state.get("error"),
                llm_calls_used=state.get("llm_calls_used", 0),
            )
            return {
                "metrics": metrics,
                "error": state.get("error"),
                "final_status": "failed",
                "llm_calls_used": state.get("llm_calls_used", 0),
            }

        status = state.get("final_status", "succeeded")
        if metrics.review_items_created > 0 and metrics.records_upserted == 0:
            status = "partial"

        self._repository.finish_crawl_run(
            run_id=run_id,
            status=status,
            metrics=metrics,
            page_analysis=page_analysis_payload,
            classification=classification_payload,
            extraction_metadata=extraction_metadata,
        )
        log_event(
            LOGGER,
            "crawl_run_persisted",
            run_id=run_id,
            status=status,
            records_upserted=metrics.records_upserted,
            review_items_created=metrics.review_items_created,
            field_jobs_created=metrics.field_jobs_created,
            llm_calls_used=state.get("llm_calls_used", 0),
        )

        return {
            "metrics": metrics,
            "final_status": status,
            "llm_calls_used": state.get("llm_calls_used", 0),
        }

    def _build_stub_records(self, state: CrawlGraphState) -> list:
        source = state["source"]
        contact_hints = state.get("chapter_contact_hints", {})
        followed_page_records = self._build_follow_page_records(state)
        records: list[ExtractedChapter] = []
        for stub in state.get("chapter_stubs", []):
            key = _stub_key(stub.chapter_name, stub.university_name)
            nested_records = followed_page_records.get(key, [])
            if nested_records:
                records.extend(nested_records)
                continue
            hints = contact_hints.get(key, {})
            source_url = sanitize_as_website(stub.detail_url or stub.outbound_chapter_url_candidate, base_url=source.list_url) or source.list_url
            outbound = stub.outbound_chapter_url_candidate or ""
            website_hint = hints.get("website_url")
            email_hint = hints.get("email")
            instagram_hint = hints.get("instagram_url")

            sanitized_website = sanitize_as_website(website_hint or outbound, base_url=source_url)
            sanitized_email = sanitize_as_email(email_hint) or sanitize_as_email(outbound)
            sanitized_instagram = sanitize_as_instagram(instagram_hint) or sanitize_as_instagram(outbound)

            records.append(
                ExtractedChapter(
                    name=stub.chapter_name,
                    university_name=stub.university_name,
                    city=None,
                    state=None,
                    website_url=sanitized_website,
                    instagram_url=sanitized_instagram,
                    contact_email=sanitized_email,
                    external_id=None,
                    source_url=source_url,
                    source_snippet=stub.provenance,
                    source_confidence=stub.confidence,
                )
            )
        return self._dedupe_extracted_records(records)

    def _build_follow_page_records(self, state: CrawlGraphState) -> dict[str, list[ExtractedChapter]]:
        source = state["source"]
        adapter = self._registry.get("repeated_block")
        if adapter is None:
            return {}

        records_by_stub: dict[str, list[ExtractedChapter]] = {}
        for stub in state.get("chapter_stubs", []):
            key = _stub_key(stub.chapter_name, stub.university_name)
            followed_pages = state.get("chapter_follow_pages", {}).get(key, [])
            if not followed_pages:
                continue

            nested_records: list[ExtractedChapter] = []
            for page_url, html in followed_pages:
                try:
                    parsed_records = adapter.parse(
                        html,
                        page_url,
                        http_client=self._http,
                        source_metadata=source.metadata,
                    )
                except Exception:
                    parsed_records = []
                nested_records.extend(parsed_records)

            if nested_records:
                records_by_stub[key] = self._dedupe_extracted_records(nested_records)

        return records_by_stub

    def _dedupe_extracted_records(self, records: list[ExtractedChapter]) -> list[ExtractedChapter]:
        deduped: dict[tuple[str, str], ExtractedChapter] = {}
        for record in records:
            key = (
                (record.name or "").strip().lower(),
                (record.university_name or "").strip().lower(),
            )
            current = deduped.get(key)
            record_tuple = (
                getattr(record, "source_confidence", 0.0),
                sum(
                    1
                    for value in (
                        record.university_name,
                        record.city,
                        record.state,
                        record.website_url,
                        record.instagram_url,
                        record.contact_email,
                    )
                    if value
                ),
            )
            current_tuple = (
                getattr(current, "source_confidence", 0.0),
                sum(
                    1
                    for value in (
                        current.university_name,
                        current.city,
                        current.state,
                        current.website_url,
                        current.instagram_url,
                        current.contact_email,
                    )
                    if value
                ),
            ) if current is not None else (-1.0, -1)
            if current is None or record_tuple > current_tuple:
                deduped[key] = record
        return list(deduped.values())

    def _merge_extracted_records(self, extracted: list, fallback_records: list) -> list:
        if not fallback_records:
            return extracted
        merged: dict[tuple[str, str], object] = {}
        for record in [*extracted, *fallback_records]:
            key = (
                (record.name or "").strip().lower(),
                (record.university_name or "").strip().lower(),
            )
            current = merged.get(key)
            if current is None or getattr(record, "source_confidence", 0.0) > getattr(current, "source_confidence", 0.0):
                merged[key] = record
        return list(merged.values())



def _to_serializable(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if is_dataclass(value):
        return _sanitize_json_value(asdict(value))
    if isinstance(value, dict):
        return _sanitize_json_value(value)
    return None


def _sanitize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _sanitize_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_json_value(item) for item in value]
    if isinstance(value, str):
        return value.replace("\x00", "")
    return value


def _stub_key(chapter_name: str, university_name: str | None) -> str:
    import re

    chapter = re.sub(r"[^a-z0-9]+", "-", chapter_name.lower()).strip("-")
    school = re.sub(r"[^a-z0-9]+", "-", (university_name or "").lower()).strip("-")
    return f"{chapter}:{school}"








