# Proper Benchmark Validation - 2026-04-03

## Objective
Validate that benchmark comparisons are methodologically sound and not dominated by timeout clipping or queue-drift artifacts.

## Benchmark Protocol Used
- Sequential execution (no parallel run overlap)
- Same config across control/treatment except runtime mode
- Same field and source scope
- Gate report must pass `Comparison quality`
  - queue-start drift <= 15%
  - full cycle completion

## Runtime/Config
- Field: `find_email`
- Source: `all` (`sourceSlug = null`)
- Workers: `4`
- Limit per cycle: `15`
- Cycles: `1`
- Pause: `0ms`
- Control runtime: `legacy`
- Treatment runtime: `langgraph_shadow`

## Runs
- Legacy control: `34f6203e-dfd4-4cfa-be61-07a8aea29cc3`
- LangGraph shadow: `8377421d-accf-46a5-8447-639aff312062`

## Results
### Legacy (`34f6203e-dfd4-4cfa-be61-07a8aea29cc3`)
- Status: `succeeded`
- Elapsed: `74,801 ms`
- Processed: `9`
- Requeued: `6`
- Failed terminal: `0`
- Jobs/min: `7.22`
- Queue delta: `+9`

### LangGraph Shadow (`8377421d-accf-46a5-8447-639aff312062`)
- Status: `succeeded`
- Elapsed: `81,019 ms`
- Processed: `3`
- Requeued: `12`
- Failed terminal: `0`
- Jobs/min: `2.22`
- Queue delta: `+3`
- Shadow diff observed jobs: `15`
- Shadow mismatch rate: `0.00%`

## Gate Report (from run API)
- Comparison quality: `PASS` (`queue-start drift 0.5%; cycles 1/1`)
- Throughput uplift: `FAIL` (`-69.2%` vs target `>= 30%`)
- Retry waste reduction: `FAIL` (`-100.0%` vs target `>= 60%`)
- p95 latency improvement: `FAIL` (`-8.3%` vs target `>= 25%`)
- Queue burn retention: `PASS` (`100.0%` vs target `>= 90%`)
- Terminal rate safety: `PASS` (`0.00% vs legacy 0.00%`)

## Benchmark Integrity Conclusion
This pair qualifies as a **proper benchmark** under the updated gate criteria because:
- The pair is sequential and config-matched.
- Queue-start drift is low (0.5%).
- Both runs completed the requested cycle count.
- Shadow diff collected real observations (`15` jobs) with zero mismatch.

## Operational Notes
- During this validation window, benchmark cycles were stabilized with `BENCHMARK_CYCLE_TIMEOUT_MS=300000` in the web server process to prevent false timeout clipping under variable provider latency.
