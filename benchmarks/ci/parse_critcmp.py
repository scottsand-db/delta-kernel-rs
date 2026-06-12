#!/usr/bin/env python3
"""
Parse critcmp output and format it as a GitHub-flavoured Markdown comment.

Reads the output of `critcmp base changes` from stdin and writes:
  1. The comment's header line, ending with a pass/fail verdict that mirrors
     the regression-gate outcome (BENCH_IGNORE_FAILURE=true renders the
     label-override variant).
  2. A one-line summary of how many benchmarks fell into each tier, ordered
     fastest to slowest.
  3. A `<details>` block (closed by default) containing the full
     per-benchmark table with columns: Test | Change | Base | PR.
  4. The tier legend as a small-text footer line.

Each benchmark is bucketed by its PR/base duration ratio into a tier with a
marker emoji (see the threshold constants). When any benchmark lands in the
slowest tier, the ratio crossed FAIL_THRESHOLD: the run records a regression in
the file named by BENCH_REGRESSION_FILE (if set) so the workflow can fail the
job.

Usage:
    critcmp base changes | python3 benchmarks/ci/parse_critcmp.py
"""
import os
import re
import sys

# Ratio = PR duration / base duration. < 1.0 is faster, > 1.0 is slower.
ROCKET_THRESHOLD = 0.85   # ratio <= 0.85: at least 15% faster.
GRAY_THRESHOLD = 1.03     # ratio <= 1.03: no worse than 3% slower.
FAIL_THRESHOLD = 1.15     # ratio >= 1.15: at least 15% slower (fails the job).

# |ratio - 1| within this renders as "1.00x" with no faster/slower suffix.
# Half of the 2-decimal display step, so the neutral band is exactly the
# ratios that round to 1.00x; the tier boundaries below reuse it so a row's
# emoji always agrees with its displayed ratio.
NEUTRAL_THRESHOLD = 0.005

# Tier markers for the Change cell, fastest to slowest.
ROCKET = '🚀'         # ratio <= ROCKET_THRESHOLD
GREEN_CHECK = '✅'    # ROCKET_THRESHOLD < ratio < 1.0 + NEUTRAL_THRESHOLD
GRAY_CHECK = '☑️'     # 1.0 + NEUTRAL_THRESHOLD <= ratio <= GRAY_THRESHOLD
CONSTRUCTION = '🚧'   # GRAY_THRESHOLD < ratio < FAIL_THRESHOLD
RED_X = '❌'          # FAIL_THRESHOLD <= ratio

# Fastest to slowest; drives both the summary order and the legend.
TIERS = [ROCKET, GREEN_CHECK, GRAY_CHECK, CONSTRUCTION, RED_X]

def to_ms(value, units):
    """Convert a critcmp duration to milliseconds.

    Matches exactly the units critcmp can emit (see `time()` in critcmp's
    output.rs): ns, µs (U+00B5), ms, s. An unrecognized unit means the critcmp
    output format changed; raise so the run fails loudly instead of rendering a
    silently wrong table.
    """
    u = units.strip()
    if u == 's':
        return value * 1e3
    if u == 'ms':
        return value
    if u == 'µs':
        return value / 1e3
    if u == 'ns':
        return value / 1e6
    raise ValueError(f'unrecognized critcmp time unit: {units!r}')

def parse_duration(s):
    m = re.match(r'([0-9.]+)±([0-9.]+)(.+)', s.strip())
    if not m:
        return None
    return float(m.group(1)), float(m.group(2)), m.group(3).strip()

def parse_rows(lines):
    """Parse critcmp stdout into a list of row dicts.

    Each row dict contains:
      name:         sanitized benchmark name (no backticks/pipes), unwrapped
      base_display: base duration string or 'N/A'
      chg_display:  changes duration string or 'N/A'
      ratio:        chg_ms / base_ms, or None if either side is missing/zero
    """
    rows = []
    for line in lines[2:]:  # skip critcmp header rows
        if not line.strip():
            continue
        # critcmp columns (split on 2+ spaces):
        #   with throughput:    name, baseFactor, baseDuration, baseBandwidth, changesFactor, changesDuration, changesBandwidth
        #   without throughput: name, baseFactor, baseDuration, changesFactor, changesDuration
        # Locate duration fields by the presence of "±" rather than hardcoding indices,
        # so the script works correctly regardless of whether bandwidth columns are present.
        fields = re.split(r'  +', line)
        name = fields[0].strip() if fields else ''
        dur_fields = [f.strip() for f in fields[1:] if '±' in f]
        base_dur_str = dur_fields[0] if len(dur_fields) > 0 else None
        chg_dur_str  = dur_fields[1] if len(dur_fields) > 1 else None

        if not name and not base_dur_str and not chg_dur_str:
            continue

        # N/A when a benchmark only exists in one of the two runs (added or removed).
        base_display = base_dur_str or 'N/A'
        chg_display  = chg_dur_str  or 'N/A'
        ratio = None

        if base_dur_str and chg_dur_str:
            base_p = parse_duration(base_dur_str)
            chg_p  = parse_duration(chg_dur_str)
            if base_p and chg_p:
                base_ms = to_ms(base_p[0], base_p[2])
                chg_ms  = to_ms(chg_p[0],  chg_p[2])

                # Float-equality on zero is safe here: to_ms only multiplies/divides
                # by powers of ten, so a zero output strictly implies a zero input.
                # Do NOT replace with an epsilon -- that would tag legitimately fast
                # benches (sub-nanosecond rounding) as N/A.
                if base_ms != 0 and chg_ms != 0:
                    ratio = chg_ms / base_ms

        rows.append({
            'name': name,
            'base_display': base_display,
            'chg_display': chg_display,
            'ratio': ratio,
        })
    return rows

def format_difference(ratio):
    """Render a ratio as e.g. '1.00x', '1.50x slower', or '2.00x faster'."""
    if ratio is None:
        return 'N/A'
    if abs(ratio - 1.0) < NEUTRAL_THRESHOLD:
        return '1.00x'
    if ratio > 1:
        return f'{ratio:.2f}x slower'
    return f'{1.0 / ratio:.2f}x faster'

def change_emoji(ratio):
    """Pick the tier marker for the Change cell. Empty string for N/A rows."""
    if ratio is None:
        return ''
    if ratio <= ROCKET_THRESHOLD:
        return ROCKET
    if ratio < 1.0 + NEUTRAL_THRESHOLD:
        return GREEN_CHECK
    if ratio <= GRAY_THRESHOLD:
        return GRAY_CHECK
    if ratio < FAIL_THRESHOLD:
        return CONSTRUCTION
    return RED_X

def render_verdict(regressed, ignored):
    """Render the pass/fail verdict shown in the comment's header line,
    mirroring the job's regression-gate outcome."""
    pct = f'{(FAIL_THRESHOLD - 1.0) * 100:.0f}%'
    if not regressed:
        return '✅ Pass'
    if ignored:
        return f'⚠️ Pass ({pct}+ regression ignored)'
    return f'❌ Fail (a benchmark regressed {pct}+)'

def render_summary(rows):
    """Render the per-tier counts on one line, fastest to slowest. N/A rows
    (added/removed benchmarks) are appended only when present."""
    counts = {tier: 0 for tier in TIERS}
    na = 0
    for r in rows:
        emoji = change_emoji(r['ratio'])
        if emoji:
            counts[emoji] += 1
        else:
            na += 1
    parts = [f"{tier} {counts[tier]}" for tier in TIERS]
    if na:
        parts.append(f"N/A {na}")
    return "**Summary:** " + " &nbsp;·&nbsp; ".join(parts)

def render_legend():
    """Render the tier legend as a small-text line for the comment footer."""
    return (
        f"<sub>**Legend:** {ROCKET} ≥15% faster &nbsp;·&nbsp;"
        f"{GREEN_CHECK} faster or unchanged &nbsp;·&nbsp;"
        f"{GRAY_CHECK} ≤3% slower &nbsp;·&nbsp;"
        f"{CONSTRUCTION} 3-15% slower &nbsp;·&nbsp;"
        f"{RED_X} ≥15% slower</sub>"
    )

def render_table(rows):
    """Render the per-benchmark table wrapped in a closed-by-default <details> block."""
    out = []
    out.append("<details>")
    out.append(f"<summary>Per-benchmark results ({len(rows)} rows)</summary>")
    out.append("")
    out.append("| Test | Change | Base         | PR               |")
    out.append("|------|--------|--------------|------------------|")
    for r in rows:
        name_cell = f"`{r['name']}`" if r['name'] else ''
        difference = format_difference(r['ratio'])
        emoji = change_emoji(r['ratio'])
        # Non-breaking spaces keep the marker and ratio on one line so the
        # Change cell renders without wrapping.
        change_cell = f"{emoji} {difference}".strip().replace(" ", "&nbsp;")
        out.append(f"| {name_cell} | {change_cell} | {r['base_display']} | {r['chg_display']} |")
    out.append("")
    out.append("</details>")
    return "\n".join(out)

def main():
    lines = sys.stdin.read().splitlines()
    rows = parse_rows(lines)
    regressed = any(r['ratio'] is not None and r['ratio'] >= FAIL_THRESHOLD for r in rows)
    ignored = os.environ.get('BENCH_IGNORE_FAILURE') == 'true'

    print(f'## Benchmark results: {render_verdict(regressed, ignored)}')
    print("")
    print(render_summary(rows))
    print("")
    print(render_table(rows))
    print("")
    print(render_legend())

    # Record whether any benchmark crossed FAIL_THRESHOLD so the workflow can
    # fail the job (unless overridden by the ignore-benchmark-failure label).
    flag_path = os.environ.get('BENCH_REGRESSION_FILE')
    if flag_path:
        with open(flag_path, 'w') as f:
            f.write('true' if regressed else 'false')

if __name__ == "__main__":
    main()
