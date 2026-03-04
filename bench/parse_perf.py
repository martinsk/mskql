#!/usr/bin/env python3
"""
parse_perf.py — Parse xctrace XML exports and produce a human-readable
CPU cache/memory performance summary for mskql benchmarks.

Usage:
    python3 parse_perf.py \
        --bench large_sort \
        --counters counters_agg.xml \
        --time-profile time_profile.xml \
        --time-samples time_samples.xml \
        --output summary.txt
"""

import argparse
import os
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime


def get_system_info():
    """Gather system info for the report header."""
    info = {}
    try:
        import subprocess
        r = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                           capture_output=True, text=True, timeout=5)
        info["cpu"] = r.stdout.strip() if r.returncode == 0 else "Unknown"
    except Exception:
        info["cpu"] = "Unknown"
    try:
        import subprocess
        l1 = subprocess.run(["sysctl", "-n", "hw.l1dcachesize"],
                            capture_output=True, text=True, timeout=5)
        l2 = subprocess.run(["sysctl", "-n", "hw.l2cachesize"],
                            capture_output=True, text=True, timeout=5)
        info["l1d"] = format_bytes(int(l1.stdout.strip())) if l1.returncode == 0 else "?"
        info["l2"] = format_bytes(int(l2.stdout.strip())) if l2.returncode == 0 else "?"
    except Exception:
        info["l1d"] = "?"
        info["l2"] = "?"
    info["date"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    return info


def format_bytes(n):
    if n >= 1024 * 1024:
        return f"{n // (1024*1024)}MB"
    if n >= 1024:
        return f"{n // 1024}KB"
    return f"{n}B"


def format_number(n):
    """Format integer with comma separators."""
    return f"{n:,}"


# ── CPU Counters parsing ─────────────────────────────────────────


def resolve_ref(elem, id_cache):
    """Resolve xctrace XML ref= elements to their original values."""
    ref = elem.get("ref")
    if ref:
        return id_cache.get(ref)
    eid = elem.get("id")
    # Extract text or fmt attribute
    val = elem.get("fmt", elem.text or "")
    if eid:
        id_cache[eid] = val
    return val


def parse_counters_xml(path):
    """Parse MetricAggregationForThread XML export.

    Returns dict of metric_name -> { 'cycles': int, 'values': [float, ...] }
    aggregated across all time intervals for the main process thread.
    """
    if not path or not os.path.exists(path):
        return None

    try:
        tree = ET.parse(path)
    except ET.ParseError:
        # Try reading raw and fixing common issues
        with open(path, "r") as f:
            content = f.read()
        if not content.strip():
            return None
        try:
            tree = ET.ElementTree(ET.fromstring(content))
        except ET.ParseError:
            return None

    root = tree.getroot()
    id_cache = {}

    # Collect all rows
    metrics = defaultdict(lambda: {"total_int": 0, "values": [], "count": 0})

    for row in root.iter("row"):
        children = list(row)
        # Extract fields by tag type
        metric_name = None
        metric_int = 0
        metric_double = 0.0
        is_precise = None

        for child in children:
            tag = child.tag
            val = resolve_ref(child, id_cache)
            if val is None:
                continue

            if tag == "string":
                metric_name = val
            elif tag == "uint64":
                try:
                    metric_int = int(val.replace(",", ""))
                except (ValueError, AttributeError):
                    metric_int = 0
            elif tag == "fixed-decimal":
                try:
                    metric_double = float(val)
                except (ValueError, AttributeError):
                    metric_double = 0.0
            elif tag == "boolean":
                is_precise = val

        if metric_name:
            m = metrics[metric_name]
            m["total_int"] += metric_int
            if metric_double != 0.0:
                m["values"].append(metric_double)
            m["count"] += 1

    if not metrics:
        return None

    return dict(metrics)


def summarize_counters(metrics):
    """Produce bottleneck summary from parsed counter metrics."""
    if not metrics:
        return None

    result = {}

    # Total cycles
    if "cycle" in metrics:
        result["cycles"] = metrics["cycle"]["total_int"]

    # Bottleneck fractions (averaged across samples, then normalized)
    # Apple Recount metrics are per-sample ratios that don't naturally
    # sum to 1.0 — normalize so they express share of total pipeline.
    raw = {}
    for name in ("useful", "delivery", "processing", "discarded"):
        if name in metrics and metrics[name]["values"]:
            vals = metrics[name]["values"]
            raw[name] = sum(vals) / len(vals)
        else:
            raw[name] = 0.0

    total = sum(raw.values())
    if total > 0:
        for name in raw:
            result[name] = raw[name] / total
    else:
        for name in raw:
            result[name] = 0.0

    return result


# ── Time Profiler parsing ─────────────────────────────────────────


def parse_time_samples_xml(path):
    """Parse time-sample XML export.

    Returns list of (function_name, sample_count) sorted by count desc.
    """
    if not path or not os.path.exists(path):
        return None

    try:
        with open(path, "r") as f:
            content = f.read()
    except Exception:
        return None

    if not content.strip():
        return None

    # The time-sample schema has backtrace frames. We want the leaf (innermost) function.
    # Each <row> has a <backtrace> with <frame> elements; the first frame is the leaf.
    # But the XML format varies — sometimes it's flat with a "symbol" or "addr" column.

    func_counts = defaultdict(int)
    total_samples = 0

    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return None

    id_cache = {}

    for row in root.iter("row"):
        children = list(row)
        # Look for backtrace or symbol information
        for child in children:
            val = resolve_ref(child, id_cache)

            # Check for backtrace element
            if child.tag == "backtrace":
                frames = list(child)
                if frames:
                    # First frame is the leaf (innermost function)
                    frame = frames[0]
                    name = frame.get("name", frame.get("fmt", ""))
                    if name:
                        func_counts[name] += 1
                        total_samples += 1
                break  # Only process first backtrace per row

    if not func_counts:
        # Fallback: try parsing time-profile schema which has a different structure
        return None

    # Sort by count descending
    sorted_funcs = sorted(func_counts.items(), key=lambda x: -x[1])
    return sorted_funcs, total_samples


def parse_time_profile_xml(path):
    """Parse time-profile XML export (call tree).

    Returns list of (function_name, weight) sorted by weight desc.
    """
    if not path or not os.path.exists(path):
        return None

    try:
        with open(path, "r") as f:
            content = f.read()
    except Exception:
        return None

    if not content.strip():
        return None

    func_weights = defaultdict(int)
    total_weight = 0

    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return None

    id_cache = {}

    for row in root.iter("row"):
        children = list(row)
        symbol_name = None
        weight = 0

        for child in children:
            val = resolve_ref(child, id_cache)
            if val is None:
                continue
            tag = child.tag

            if tag == "string" or tag == "symbol":
                symbol_name = val
            elif tag == "sample-count" or tag == "count":
                try:
                    weight = int(val.replace(",", ""))
                except (ValueError, AttributeError):
                    pass
            elif tag == "uint64":
                # Could be sample count
                try:
                    weight = int(val.replace(",", ""))
                except (ValueError, AttributeError):
                    pass

        if symbol_name and weight > 0:
            func_weights[symbol_name] += weight
            total_weight += weight

    if not func_weights:
        return None

    sorted_funcs = sorted(func_weights.items(), key=lambda x: -x[1])
    return sorted_funcs, total_weight


def try_parse_functions(time_profile_path, time_samples_path):
    """Try both time-profile and time-sample parsing, return whichever works."""
    result = parse_time_samples_xml(time_samples_path)
    if result and result[0]:
        return result

    result = parse_time_profile_xml(time_profile_path)
    if result and result[0]:
        return result

    # Last resort: grep for function names in either XML file
    for path in [time_samples_path, time_profile_path]:
        if not path or not os.path.exists(path):
            continue
        try:
            with open(path, "r") as f:
                content = f.read()
        except Exception:
            continue

        # Look for fmt="..." attributes that look like function names
        func_counts = defaultdict(int)
        # Pattern: C function names in the binary
        for m in re.finditer(r'name="([a-z_][a-z0-9_]*)"', content, re.IGNORECASE):
            name = m.group(1)
            # Filter out XML schema names and common noise
            if name in ("timestamp", "duration", "thread", "process",
                        "metric-value-int", "metric-value-double",
                        "metric-name", "is-precise", "string", "uint64",
                        "fixed-decimal", "boolean", "start-time",
                        "sample-count", "count", "backtrace", "frame"):
                continue
            func_counts[name] += 1

        if func_counts:
            sorted_funcs = sorted(func_counts.items(), key=lambda x: -x[1])
            total = sum(c for _, c in sorted_funcs)
            return sorted_funcs, total

    return None


# ── Report generation ─────────────────────────────────────────────


def generate_report(bench_name, counter_summary, func_data, sys_info):
    """Generate the human-readable performance report."""
    lines = []
    W = 60  # report width

    lines.append("═" * W)
    lines.append(f"  mskql perf analysis: {bench_name}")
    cpu_short = sys_info.get("cpu", "Unknown").replace("Apple ", "")
    lines.append(f"  {cpu_short} | L1D={sys_info.get('l1d','?')} L2={sys_info.get('l2','?')} | {sys_info.get('date','')}")
    lines.append("═" * W)
    lines.append("")

    # ── Bottleneck summary ────────────────────────────────────────
    if counter_summary:
        lines.append("  BOTTLENECK SUMMARY (Apple CPU Counters)")
        lines.append("  " + "─" * (W - 4))

        if "cycles" in counter_summary and counter_summary["cycles"] > 0:
            lines.append(f"  Total cycles:       {format_number(counter_summary['cycles'])}")

        labels = [
            ("useful",     "actual work done"),
            ("delivery",   "waiting for cache/memory"),
            ("processing", "branch mispredict / dep chain"),
            ("discarded",  "speculative work thrown away"),
        ]

        for key, desc in labels:
            if key in counter_summary:
                pct = counter_summary[key] * 100
                bar_len = int(pct / 100 * 30)
                bar = "█" * bar_len + "░" * (30 - bar_len)
                lines.append(f"  {key:<14s} {pct:5.1f}%  {bar}  ← {desc}")

        lines.append("")

        # Analysis paragraph
        lines.append("  INTERPRETATION")
        lines.append("  " + "─" * (W - 4))

        delivery = counter_summary.get("delivery", 0) * 100
        processing = counter_summary.get("processing", 0) * 100
        useful = counter_summary.get("useful", 0) * 100
        discarded = counter_summary.get("discarded", 0) * 100

        if delivery > 25:
            lines.append(f"  ⚠ Delivery stall is HIGH ({delivery:.1f}%) — the CPU is frequently")
            lines.append(f"    waiting for data from cache/memory. This suggests cache misses")
            lines.append(f"    are a significant bottleneck. Look for random-access patterns")
            lines.append(f"    in hot functions (hash table probes, sort comparisons).")
        elif delivery > 15:
            lines.append(f"  ◆ Delivery stall is MODERATE ({delivery:.1f}%) — some cache pressure.")
            lines.append(f"    There may be optimization opportunities in memory access patterns.")
        else:
            lines.append(f"  ✓ Delivery stall is LOW ({delivery:.1f}%) — not memory-bound.")

        if processing > 20:
            lines.append(f"  ⚠ Processing stall is HIGH ({processing:.1f}%) — branch mispredictions")
            lines.append(f"    or long dependency chains. Consider branchless comparisons.")
        elif processing > 10:
            lines.append(f"  ◆ Processing stall is MODERATE ({processing:.1f}%).")

        if discarded > 15:
            lines.append(f"  ⚠ Discarded work is HIGH ({discarded:.1f}%) — lots of speculative")
            lines.append(f"    execution being thrown away. Data-dependent branches are the")
            lines.append(f"    likely cause.")

        if useful > 70:
            lines.append(f"  ✓ Useful work is {useful:.1f}% — this workload is fairly efficient.")
        elif useful > 50:
            lines.append(f"  ◆ Useful work is {useful:.1f}% — room for improvement.")
        else:
            lines.append(f"  ⚠ Useful work is only {useful:.1f}% — significant overhead.")

        lines.append("")
    else:
        lines.append("  BOTTLENECK SUMMARY: (no CPU counter data available)")
        lines.append("")

    # ── Top functions ─────────────────────────────────────────────
    if func_data:
        sorted_funcs, total = func_data
        lines.append("  TOP FUNCTIONS (Time Profiler)")
        lines.append("  " + "─" * (W - 4))

        # Show top 15 functions, filtering noise
        shown = 0
        for func_name, count in sorted_funcs:
            if shown >= 15:
                break
            # Skip system/runtime noise
            skip_prefixes = ("_dispatch_", "_pthread_", "__psynch_",
                             "_mach_", "start_wqthread", "thread_start",
                             "_sigtramp", "dyld", "_os_", "__ulock_")
            if any(func_name.startswith(p) for p in skip_prefixes):
                continue
            if func_name in ("0x0", "(null)", ""):
                continue

            pct = (count / total * 100) if total > 0 else 0
            shown += 1
            bar_len = int(pct / 100 * 20)
            bar = "█" * bar_len + "░" * (20 - bar_len)
            lines.append(f"  {shown:2d}. {func_name:<35s} {pct:5.1f}%  {bar}")

        if shown == 0:
            lines.append("  (no user functions found in samples)")

        lines.append("")
    else:
        lines.append("  TOP FUNCTIONS: (no time profiler data available)")
        lines.append("")

    # ── Optimization suggestions ──────────────────────────────────
    if counter_summary and func_data:
        lines.append("  OPTIMIZATION TARGETS")
        lines.append("  " + "─" * (W - 4))

        delivery = counter_summary.get("delivery", 0) * 100
        sorted_funcs, total = func_data

        if sorted_funcs and delivery > 10:
            top_func = sorted_funcs[0][0]
            top_pct = sorted_funcs[0][1] / total * 100 if total > 0 else 0
            lines.append(f"  Hottest function: {top_func} ({top_pct:.1f}% of samples)")
            lines.append(f"  With {delivery:.1f}% delivery stall, focus on this function's")
            lines.append(f"  memory access pattern — prefetching, data layout, or reducing")
            lines.append(f"  pointer chasing could help.")
        elif sorted_funcs:
            top_func = sorted_funcs[0][0]
            top_pct = sorted_funcs[0][1] / total * 100 if total > 0 else 0
            lines.append(f"  Hottest function: {top_func} ({top_pct:.1f}% of samples)")
            lines.append(f"  Delivery stall is low — focus on algorithmic improvements")
            lines.append(f"  or reducing instruction count in the hot function.")

        lines.append("")

    lines.append("═" * W)
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Parse xctrace perf data")
    parser.add_argument("--bench", required=True, help="Benchmark name")
    parser.add_argument("--counters", help="Path to counters_agg.xml")
    parser.add_argument("--time-profile", help="Path to time_profile.xml")
    parser.add_argument("--time-samples", help="Path to time_samples.xml")
    parser.add_argument("--output", help="Output file (default: stdout)")
    args = parser.parse_args()

    sys_info = get_system_info()

    # Parse counter data
    metrics = parse_counters_xml(args.counters)
    counter_summary = summarize_counters(metrics)

    # Parse function data
    func_data = try_parse_functions(args.time_profile, args.time_samples)

    # Generate report
    report = generate_report(args.bench, counter_summary, func_data, sys_info)

    if args.output:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w") as f:
            f.write(report + "\n")
        # Also print to stdout
        print(report)
    else:
        print(report)


if __name__ == "__main__":
    main()
