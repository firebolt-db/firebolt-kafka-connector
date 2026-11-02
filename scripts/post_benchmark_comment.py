#!/usr/bin/env python3
"""Build the benchmark PR comment body from result JSON files and print it to stdout."""
import json

CELLS  = ['at_least_once-sql', 'exactly_once-sql']
LABELS = {'at_least_once-sql': 'AT_LEAST_ONCE', 'exactly_once-sql': 'EXACTLY_ONCE'}

results = {}
for cell in CELLS:
    try:
        with open(f'build/reports/benchmark/results-{cell}.json') as f:
            results[cell] = json.load(f)
    except OSError:
        pass

baseline = {}
try:
    with open('specs/benchmarks/baseline.json') as f:
        baseline = json.load(f)
except OSError:
    pass

def fmt(n):
    return f'{int(n):,}'

def delta(cur, base):
    if not base:
        return ''
    pct = (cur - base) / base * 100
    # ±10%: run-to-run noise on shared CI runners, even with the fixed-workload method.
    icon = ' 🚀' if pct >= 10 else (' ✅' if pct >= -10 else ' ⚠️')
    return f'{pct:+.1f}%{icon}'

any_r  = next(iter(results.values()), {})
sha    = (any_r.get('commit_sha') or 'unknown')[:7]
any_b  = next(iter(baseline.values()), {}) if baseline else {}
bsha   = (any_b.get('commit_sha') or '')[:7] or None
# Only compare runs measured the same way (see BenchmarkResult.method).
has_b  = bool(baseline and bsha and any_b.get('method') == any_r.get('method'))

if has_b:
    rows = ['| Delivery | Ingest rate | Ingest throughput | vs Baseline |',
            '|----------|-------------|-------------------|-------------|']
else:
    rows = ['| Delivery | Ingest rate | Ingest throughput | Records |',
            '|----------|-------------|-------------------|---------|']

for cell in CELLS:
    r = results.get(cell)
    if not r:
        continue
    b     = baseline.get(cell, {})
    rate  = fmt(r['ingest_rate_records_per_sec'])
    mb    = r['ingest_throughput_mb_per_sec']
    label = LABELS[cell]
    if has_b:
        d = delta(r['ingest_rate_records_per_sec'], b.get('ingest_rate_records_per_sec'))
        rows.append(f'| {label} | {rate} rec/s | {mb} MB/s | {d} |')
    else:
        rows.append(f'| {label} | {rate} rec/s | {mb} MB/s | {fmt(r["total_records_produced"])} |')

rec_size  = any_r.get('record_size_bytes', 256)
total     = any_r.get('total_records_produced', 0)
overrides = any_r.get('connector_overrides') or {}
b_note    = (f' · baseline: main @ `{bsha}`' if has_b
             else ' · no comparable baseline on main yet (measurement method changed)' if baseline else '')
cfg       = ', '.join(f'`{k.replace("consumer.override.", "")}={v}`' for k, v in sorted(overrides.items()))

print('\n'.join([
    '## Throughput Benchmark Results',
    '',
    f'_This PR: `{sha}`{b_note}_',
    '',
    *rows,
    '',
    f'_Schemaless JSON · {fmt(total)} × {rec_size}B records drained from a paused connector'
    + (f' · {cfg}' if cfg else '') + '_',
]))
