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
    icon = ' 🚀' if pct >= 5 else (' ✅' if pct >= -5 else ' ⚠️')
    return f'{pct:+.1f}%{icon}'

any_r  = next(iter(results.values()), {})
sha    = (any_r.get('commit_sha') or 'unknown')[:7]
any_b  = next(iter(baseline.values()), {}) if baseline else {}
bsha   = (any_b.get('commit_sha') or '')[:7] or None
has_b  = bool(baseline and bsha)

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

dur      = any_r.get('duration_seconds', 10)
rec_size = any_r.get('record_size_bytes', 256)
b_note   = f' · baseline: main @ `{bsha}`' if has_b else ''

print('\n'.join([
    '## Throughput Benchmark Results',
    '',
    f'_This PR: `{sha}`{b_note}_',
    '',
    *rows,
    '',
    f'_JSON / SQL · {dur}s produce window · {rec_size}B records_',
]))
