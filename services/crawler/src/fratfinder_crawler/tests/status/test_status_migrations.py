from __future__ import annotations

from pathlib import Path


MIGRATION_PATH = Path(__file__).resolve().parents[6] / "infra" / "supabase" / "migrations" / "0032_chapter_status_engine.sql"


def test_status_migration_file_contains_required_tables_and_constraints():
    sql = MIGRATION_PATH.read_text(encoding="utf-8").lower()
    assert "create table if not exists campus_status_sources" in sql
    assert "create table if not exists campus_status_zones" in sql
    assert "create table if not exists chapter_status_evidence" in sql
    assert "create table if not exists chapter_status_decisions" in sql
    assert "final_status text not null" in sql
    assert "reason_code text not null" in sql
    assert "evidence_ids uuid[] not null" in sql
    assert "decision_trace jsonb not null" in sql
    assert "source_url text not null" in sql
    assert "status_signal text not null" in sql


def test_status_migration_is_additive_and_does_not_drop_existing_status_columns():
    sql = MIGRATION_PATH.read_text(encoding="utf-8").lower()
    assert "drop table" not in sql
    assert "alter table chapters drop" not in sql
