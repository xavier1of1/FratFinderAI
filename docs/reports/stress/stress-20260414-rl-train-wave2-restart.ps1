Set-Location 'd:\VSC Programs\FratFinderAI'
python scripts\field_job_stress_harness.py --run-id stress-20260414-rl-train-wave2 --skip-enqueue --workers 8 --max-workers 12 --limit 240 --batches 6 --priority 975 --graph-durability sync --run-preflight --report-path docs\reports\stress\stress-20260414-rl-train-wave2.jsonl --recovery-promotion-limit 48 --recovery-workers 3
