#!/usr/bin/env python3
"""
parse_simd.py — Analyze clang vectorization remarks and ARM64/x86-64 assembly
to produce a graded SIMD & ILP report for mskql hot-path functions.

Usage:
    python3 parse_simd.py \
        --remarks vectorization_remarks.txt \
        --asm-dir asm/ \
        --output report.md
"""

import argparse
import os
import re
import subprocess
from collections import defaultdict
from datetime import datetime

# ── Target functions to analyze ──────────────────────────────────

TARGET_FUNCTIONS = {
    # (function_name, category, description)
    "vec_filter_i32":            ("Filter",      "INT/BOOLEAN/DATE column filter"),
    "vec_filter_i64":            ("Filter",      "BIGINT/TIMESTAMP column filter"),
    "vec_filter_i16":            ("Filter",      "SMALLINT column filter"),
    "vec_filter_f64":            ("Filter",      "FLOAT/NUMERIC column filter"),
    "filter_next":               ("Filter",      "Plan executor filter dispatch"),
    "radix_sort_u32":            ("Sort",        "Radix sort for INT keys"),
    "radix_sort_u64":            ("Sort",        "Radix sort for BIGINT keys"),
    "radix_sort_f64":            ("Sort",        "Radix sort for FLOAT keys"),
    "sort_next":                 ("Sort",        "Sort node: collect + sort + emit"),
    "hash_join_build":           ("Join",        "Hash join build side"),
    "hash_join_next":            ("Join",        "Hash join probe + emit"),
    "hash_agg_next":             ("Aggregation", "GROUP BY hash aggregation"),
    "simple_agg_next":           ("Aggregation", "Simple aggregate (no GROUP BY)"),
    "gen_series_next":           ("Generate",    "generate_series() fill loop"),
    "vec_project_next":          ("Projection",  "Vectorized arithmetic/cast"),
    "window_next":               ("Window",      "Window function computation"),
    "distinct_next":             ("Distinct",    "Hash-based DISTINCT dedup"),
    "serialize_all_int_block":   ("Wire",        "All-INT block serialization"),
    "serialize_int_bigint_block":("Wire",        "INT/BIGINT block serialization"),
    "fast_i32_to_str":           ("Wire",        "INT to decimal text conversion"),
    "fast_i64_to_str":           ("Wire",        "BIGINT to decimal text conversion"),
    "fast_f64_to_str":           ("Wire",        "FLOAT to decimal text conversion"),
    "flat_table_read":           ("Scan",        "Columnar read from flat storage"),
    "serialize_numeric_block":   ("Wire",        "Generic numeric block serializer"),
}


# ── Vectorization remarks parsing ────────────────────────────────


def parse_remarks(path):
    """Parse clang -Rpass=loop-vectorize output.

    Returns dict: { source_file: [ {line, col, kind, message, function} ] }
    where kind is 'vectorized', 'missed', or 'analysis'.
    """
    if not path or not os.path.exists(path):
        return {}

    remarks = defaultdict(list)
    # Format: /path/to/file.c:LINE:COL: remark: MESSAGE [-Rpass=...]
    # or:     /path/to/file.c:LINE:COL: remark: MESSAGE [-Rpass-missed=...]
    pattern = re.compile(
        r'^(.+?):(\d+):(\d+): (?:remark|warning): (.+?)(?:\s+\[-R(pass(?:-missed|-analysis)?)=loop-vectorize\])?$'
    )

    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            raw_line = raw_line.rstrip()
            m = pattern.match(raw_line)
            if not m:
                # Simpler fallback: just check for keywords
                if "vectorized" in raw_line.lower() or "not vectorized" in raw_line.lower():
                    src_file = raw_line.split(":")[0] if ":" in raw_line else "unknown"
                    base = os.path.basename(src_file)
                    kind = "missed" if "not vectorized" in raw_line.lower() else "vectorized"
                    remarks[base].append({
                        "line": 0, "col": 0, "kind": kind,
                        "message": raw_line, "function": ""
                    })
                continue

            src_file, line_no, col_no, message, rpass_kind = m.groups()
            base = os.path.basename(src_file)

            if rpass_kind == "pass-missed" or "not vectorized" in message.lower():
                kind = "missed"
            elif rpass_kind == "pass-analysis":
                kind = "analysis"
            else:
                kind = "vectorized"

            remarks[base].append({
                "line": int(line_no),
                "col": int(col_no),
                "kind": kind,
                "message": message.strip(),
                "function": "",
            })

    return dict(remarks)


def match_remarks_to_functions(remarks, func_line_ranges):
    """Associate remarks with target functions based on line ranges."""
    func_remarks = defaultdict(list)

    for src_file, file_remarks in remarks.items():
        ranges = func_line_ranges.get(src_file, {})
        for remark in file_remarks:
            line = remark["line"]
            matched = False
            for func_name, (start, end) in ranges.items():
                if start <= line <= end:
                    remark["function"] = func_name
                    func_remarks[func_name].append(remark)
                    matched = True
                    break
            if not matched:
                func_remarks["_unmatched"].append(remark)

    return dict(func_remarks)


# ── Assembly parsing (ARM64 NEON + x86 SSE/AVX) ─────────────────


# ARM64 NEON SIMD mnemonics (common subset)
ARM64_SIMD_MNEMONICS = {
    # Vector integer
    "add", "sub", "mul", "and", "orr", "eor", "bic", "orn",
    "shl", "sshr", "ushr", "ssra", "usra",
    "smax", "smin", "umax", "umin",
    "addv", "saddl", "uaddl", "saddw", "uaddw",
    "smull", "umull", "smlal", "umlal",
    "abs", "neg", "not", "cnt", "cls", "clz", "rev16", "rev32", "rev64",
    "cmhi", "cmhs", "cmge", "cmgt", "cmle", "cmlt", "cmeq", "cmtst",
    "tbl", "tbx", "trn1", "trn2", "zip1", "zip2", "uzp1", "uzp2",
    "ext", "dup", "ins", "mov", "movi", "mvni",
    "saddlv", "uaddlv", "smaxv", "sminv", "umaxv", "uminv",
    # Vector float
    "fadd", "fsub", "fmul", "fdiv", "fmadd", "fnmadd", "fmsub", "fnmsub",
    "fmla", "fmls", "fabs", "fneg", "fsqrt", "fmax", "fmin",
    "fmaxnm", "fminnm", "fmaxv", "fminv",
    "fcmeq", "fcmge", "fcmgt", "fcmle", "fcmlt",
    "fcvtzs", "fcvtzu", "scvtf", "ucvtf",
    "faddp", "fmaxnmp", "fminnmp",
    # Load/store SIMD
    "ld1", "ld2", "ld3", "ld4", "st1", "st2", "st3", "st4",
    "ld1r", "ld2r", "ld3r", "ld4r",
    "ldp", "stp",  # paired (may be scalar or vector)
}

# x86 SIMD prefixes (SSE, AVX, AVX-512)
X86_SIMD_PREFIXES = (
    "movaps", "movups", "movdqa", "movdqu", "movss", "movsd",
    "addps", "addpd", "subps", "subpd", "mulps", "mulpd", "divps", "divpd",
    "vpadd", "vpsub", "vpmul", "vpand", "vpor", "vpxor",
    "vmovaps", "vmovups", "vmovdqa", "vmovdqu",
    "vaddps", "vaddpd", "vsubps", "vsubpd", "vmulps", "vmulpd",
    "vfmadd", "vfmsub", "vfnmadd", "vfnmsub",
    "vcmp", "vpcmp", "vptest",
    "vpmovzx", "vpmovsx", "vpshuf", "vpsll", "vpsrl", "vpsra",
    "vpbroadcast", "vbroadcast",
    "vgather", "vpgather",
)

ARM64_BRANCH_MNEMONICS = {"b", "bl", "blr", "br", "ret", "cbz", "cbnz",
                           "tbz", "tbnz", "b.eq", "b.ne", "b.lt", "b.gt",
                           "b.le", "b.ge", "b.hi", "b.hs", "b.lo", "b.ls",
                           "b.mi", "b.pl", "b.vs", "b.vc", "b.al", "b.nv",
                           "b.cc", "b.cs"}

X86_BRANCH_MNEMONICS = {"je", "jne", "jl", "jg", "jle", "jge", "ja", "jb",
                         "jae", "jbe", "jmp", "call", "ret", "jz", "jnz",
                         "js", "jns", "jo", "jno", "jc", "jnc"}

ARM64_LOAD_STORE = {"ldr", "ldrsw", "ldrb", "ldrh", "ldrsb", "ldrsh",
                     "str", "strb", "strh", "ldp", "stp",
                     "ldar", "stlr", "ldxr", "stxr",
                     "ld1", "ld2", "ld3", "ld4", "st1", "st2", "st3", "st4",
                     "ldur", "stur", "ldurb", "sturb"}

X86_LOAD_STORE = {"mov", "movzx", "movsx", "movaps", "movups",
                   "movdqa", "movdqu", "vmovaps", "vmovups",
                   "vmovdqa", "vmovdqu", "lea"}


def detect_arch(asm_text):
    """Detect ARM64 vs x86-64 from assembly content."""
    # ARM64 uses .cfi_startproc, x0-x30 registers, etc.
    if re.search(r'\bx\d+\b', asm_text[:2000]) or re.search(r'\bw\d+\b', asm_text[:2000]):
        return "arm64"
    if re.search(r'\b[er][abcd]x\b', asm_text[:2000]) or re.search(r'\bxmm\d+\b', asm_text[:2000]):
        return "x86_64"
    return "arm64"  # default on macOS


def is_simd_instruction(mnemonic, operands, arch):
    """Check if an instruction is a SIMD instruction."""
    mn = mnemonic.lower().rstrip()

    if arch == "arm64":
        # On ARM64, NEON instructions operate on v0-v31 registers
        # Check if operands reference vector registers
        has_vreg = bool(re.search(r'\bv\d+\b', operands))
        if has_vreg:
            return True
        # Some NEON mnemonics are unique
        base_mn = mn.split(".")[0] if "." in mn else mn
        if base_mn in ARM64_SIMD_MNEMONICS and has_vreg:
            return True
        return False
    else:
        # x86: check for xmm/ymm/zmm registers or SIMD prefixes
        if re.search(r'\b[xyz]mm\d+\b', operands):
            return True
        for prefix in X86_SIMD_PREFIXES:
            if mn.startswith(prefix):
                return True
        return False


def is_branch_instruction(mnemonic, arch):
    """Check if instruction is a branch/jump."""
    mn = mnemonic.lower().rstrip()
    if arch == "arm64":
        base = mn.split(".")[0]
        return base in ARM64_BRANCH_MNEMONICS or mn in ARM64_BRANCH_MNEMONICS
    else:
        return mn in X86_BRANCH_MNEMONICS


def is_load_store(mnemonic, arch):
    """Check if instruction is a memory load or store."""
    mn = mnemonic.lower().rstrip()
    if arch == "arm64":
        base = mn.split(".")[0]
        return base in ARM64_LOAD_STORE
    else:
        return mn in X86_LOAD_STORE


def extract_functions_from_asm(asm_path):
    """Parse an assembly file and extract per-function instruction stats.

    Returns dict: { func_name: FuncStats }
    """
    if not asm_path or not os.path.exists(asm_path):
        return {}

    with open(asm_path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    arch = detect_arch("".join(lines[:100]))

    # ARM64 clang: global functions start with `_funcname:` (leading underscore on macOS)
    # Local labels are `LBB23_4:`, `Lfunc_end22:`, `Ltmp123:` — skip those
    func_label_re = re.compile(r'^(_[a-z_][a-zA-Z0-9_]*):')
    # End-of-function markers
    func_end_re = re.compile(r'^Lfunc_end\d+:')
    # Instructions: lines starting with whitespace then a mnemonic
    instr_re = re.compile(r'^\s+([a-zA-Z][a-zA-Z0-9_.]*)\s*(.*?)(?:;.*)?$')
    # Directives start with .
    directive_re = re.compile(r'^\s*\.')

    functions = {}
    current_func = None
    current_stats = None

    def new_stats():
        return {
            "total_insns": 0,
            "simd_insns": 0,
            "branch_insns": 0,
            "load_store_insns": 0,
            "prefetch_insns": 0,
            "neon_widths": defaultdict(int),
            "simd_details": defaultdict(int),
            "branch_details": defaultdict(int),
        }

    for line in lines:
        line = line.rstrip()

        # Check for end-of-function marker
        if func_end_re.match(line):
            if current_func and current_stats:
                functions[current_func] = current_stats
                current_func = None
                current_stats = None
            continue

        # Check for global function label (starts with _lowercase)
        m = func_label_re.match(line)
        if m:
            # Close previous if still open
            if current_func and current_stats:
                functions[current_func] = current_stats
            raw_name = m.group(1)
            func_name = raw_name.lstrip("_")
            if func_name in TARGET_FUNCTIONS:
                current_func = func_name
                current_stats = new_stats()
            else:
                current_func = None
                current_stats = None
            continue

        if not current_func:
            continue

        # Skip directives
        if directive_re.match(line):
            continue

        # Parse instruction
        m = instr_re.match(line)
        if not m:
            continue

        mnemonic = m.group(1)
        operands = m.group(2)

        current_stats["total_insns"] += 1

        if is_simd_instruction(mnemonic, operands, arch):
            current_stats["simd_insns"] += 1
            current_stats["simd_details"][mnemonic] += 1
            # Track NEON arrangement specifiers
            if arch == "arm64":
                for w in re.findall(r'\.\d+[bhsdBHSD]', operands):
                    current_stats["neon_widths"][w] += 1

        if is_branch_instruction(mnemonic, arch):
            current_stats["branch_insns"] += 1
            current_stats["branch_details"][mnemonic] += 1

        if is_load_store(mnemonic, arch):
            current_stats["load_store_insns"] += 1

        if "prfm" in mnemonic.lower() or "prefetch" in mnemonic.lower():
            current_stats["prefetch_insns"] += 1

    # Don't forget last function
    if current_func and current_stats:
        functions[current_func] = current_stats

    return functions


# ── Function line range extraction ───────────────────────────────


def get_func_line_ranges(src_dir):
    """Use ctags or regex to find function line ranges in source files.

    Returns dict: { "plan.c": { "vec_filter_i32": (start, end), ... }, ... }
    """
    result = {}

    for src_file in ["plan.c", "pgwire.c"]:
        path = os.path.join(src_dir, src_file)
        if not os.path.exists(path):
            continue

        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        # Find function definitions: `static TYPE func_name(` or `TYPE func_name(`
        func_starts = {}
        # Match lines like: static int vec_filter_i32(...)
        # or: static uint16_t serialize_all_int_block(...)
        func_def_re = re.compile(
            r'^(?:static\s+)?(?:inline\s+)?'
            r'(?:(?:int|void|uint16_t|uint32_t|int64_t|double|struct\s+\w+)\s+\*?\s*)'
            r'([a-zA-Z_][a-zA-Z0-9_]*)\s*\('
        )
        # Match macro-generated functions: VEC_FILTER_FUNC(name, ...)
        macro_func_re = re.compile(r'^VEC_FILTER_FUNC\((\w+),')

        for i, line in enumerate(lines):
            m = func_def_re.match(line)
            if m:
                fname = m.group(1)
                if fname in TARGET_FUNCTIONS:
                    func_starts[fname] = i + 1  # 1-indexed
            m2 = macro_func_re.match(line)
            if m2:
                fname = m2.group(1)
                if fname in TARGET_FUNCTIONS:
                    func_starts[fname] = i + 1

        # Estimate end: next function start or +500 lines
        sorted_starts = sorted(func_starts.items(), key=lambda x: x[1])
        ranges = {}
        for idx, (fname, start) in enumerate(sorted_starts):
            if idx + 1 < len(sorted_starts):
                end = sorted_starts[idx + 1][1] - 1
            else:
                end = start + 500
            ranges[fname] = (start, end)

        result[src_file] = ranges

    return result


# ── System info ──────────────────────────────────────────────────


def get_system_info():
    info = {}
    try:
        r = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                           capture_output=True, text=True, timeout=5)
        info["cpu"] = r.stdout.strip() if r.returncode == 0 else "Unknown"
    except Exception:
        info["cpu"] = "Unknown"
    try:
        r = subprocess.run(["uname", "-m"], capture_output=True, text=True, timeout=5)
        info["arch"] = r.stdout.strip() if r.returncode == 0 else "unknown"
    except Exception:
        info["arch"] = "unknown"
    try:
        cc_path = "/opt/homebrew/opt/llvm/bin/clang"
        if not os.path.exists(cc_path):
            cc_path = "clang"
        r = subprocess.run([cc_path, "--version"], capture_output=True, text=True, timeout=5)
        info["compiler"] = r.stdout.strip().split("\n")[0] if r.returncode == 0 else "Unknown"
    except Exception:
        info["compiler"] = "Unknown"
    info["date"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    return info


# ── Grading logic ────────────────────────────────────────────────


def grade_function(func_name, asm_stats, func_remarks):
    """Produce a grade and analysis for a single function."""
    grade = {
        "name": func_name,
        "category": TARGET_FUNCTIONS[func_name][0],
        "description": TARGET_FUNCTIONS[func_name][1],
        "vectorized": "unknown",
        "simd_pct": 0.0,
        "branch_pct": 0.0,
        "load_store_pct": 0.0,
        "total_insns": 0,
        "simd_insns": 0,
        "branch_insns": 0,
        "prefetch_insns": 0,
        "has_prefetch": False,
        "ilp_estimate": "unknown",
        "limiting_factors": [],
        "remark_summary": "",
        "neon_widths": {},
    }

    # Analyze remarks
    vectorized_count = 0
    missed_count = 0
    analysis_msgs = []
    if func_remarks:
        for r in func_remarks:
            if r["kind"] == "vectorized":
                vectorized_count += 1
            elif r["kind"] == "missed":
                missed_count += 1
            elif r["kind"] == "analysis":
                analysis_msgs.append(r["message"])

    if vectorized_count > 0 and missed_count == 0:
        grade["vectorized"] = "yes"
    elif vectorized_count > 0 and missed_count > 0:
        grade["vectorized"] = "partial"
    elif missed_count > 0:
        grade["vectorized"] = "no"

    remark_parts = []
    if vectorized_count:
        remark_parts.append(f"{vectorized_count} loops vectorized")
    if missed_count:
        remark_parts.append(f"{missed_count} loops missed")
    grade["remark_summary"] = ", ".join(remark_parts) if remark_parts else "no remarks"

    # Analyze assembly
    if asm_stats:
        total = asm_stats["total_insns"]
        grade["total_insns"] = total
        grade["simd_insns"] = asm_stats["simd_insns"]
        grade["branch_insns"] = asm_stats["branch_insns"]
        grade["prefetch_insns"] = asm_stats["prefetch_insns"]
        grade["has_prefetch"] = asm_stats["prefetch_insns"] > 0
        grade["neon_widths"] = dict(asm_stats.get("neon_widths", {}))

        if total > 0:
            grade["simd_pct"] = asm_stats["simd_insns"] / total * 100
            grade["branch_pct"] = asm_stats["branch_insns"] / total * 100
            grade["load_store_pct"] = asm_stats["load_store_insns"] / total * 100

        # ILP estimate based on branch density and SIMD usage
        if total > 0:
            branch_ratio = asm_stats["branch_insns"] / total
            simd_ratio = asm_stats["simd_insns"] / total
            if simd_ratio > 0.15 and branch_ratio < 0.10:
                grade["ilp_estimate"] = "high"
            elif simd_ratio > 0.05 or branch_ratio < 0.15:
                grade["ilp_estimate"] = "moderate"
            else:
                grade["ilp_estimate"] = "low"

        # Identify limiting factors
        if asm_stats["simd_insns"] == 0:
            grade["limiting_factors"].append("no SIMD instructions generated")
        if total > 0 and asm_stats["branch_insns"] / total > 0.15:
            grade["limiting_factors"].append("high branch density limits ILP")
        if total > 0 and asm_stats["load_store_insns"] / total > 0.40:
            grade["limiting_factors"].append("memory-dominated (>40% load/store)")

    # Identify known structural issues
    known_issues = {
        "vec_filter_i32": ["indirect indexing via cand[] forces gather pattern"],
        "vec_filter_i64": ["indirect indexing via cand[] forces gather pattern"],
        "vec_filter_i16": ["indirect indexing via cand[] forces gather pattern"],
        "vec_filter_f64": ["indirect indexing via cand[] forces gather pattern"],
        "filter_next":    ["large switch dispatch limits inlining"],
        "radix_sort_u32": ["histogram scatter (offsets[byte]++) is inherently serial"],
        "radix_sort_u64": ["histogram scatter is serial", "8-pass (vs 4 for u32) doubles work"],
        "radix_sort_f64": ["histogram scatter is serial", "float-to-uint64 bit manipulation"],
        "sort_next":      ["qsort/pdqsort via function pointer blocks cross-iteration opt",
                           "global _bsort_ctx adds pointer indirection to every comparison"],
        "hash_join_build": ["open-addressing probe chain is branch-heavy"],
        "hash_join_next":  ["open-addressing probe chain is branch-heavy",
                            "pointer chasing through hash table entries"],
        "hash_agg_next":   ["open-addressing hash table", "per-group accumulator updates are serial"],
        "simple_agg_next": ["accumulator dependency chain limits ILP"],
        "vec_project_next":["large switch/case dispatch per expression type",
                            "mixed types in single function inhibit specialization"],
        "serialize_all_int_block":   ["int-to-text conversion is inherently scalar"],
        "serialize_int_bigint_block":["int-to-text conversion is inherently scalar"],
        "fast_i32_to_str": ["division/modulo chain for digit extraction is serial"],
        "fast_i64_to_str": ["division/modulo chain for digit extraction is serial"],
        "fast_f64_to_str": ["complex branching for special cases"],
        "window_next":     ["qsort via function pointer", "per-row window value update is serial"],
        "distinct_next":   ["hash table probe chain is branch-heavy"],
        "flat_table_read":  ["simple memcpy — should be well-optimized by compiler"],
        "serialize_numeric_block": ["type-switch per cell per row"],
        "gen_series_next":  ["simple sequential fill — should vectorize"],
    }
    if func_name in known_issues:
        grade["limiting_factors"].extend(known_issues[func_name])

    return grade


# ── Report generation ────────────────────────────────────────────


def generate_report(grades, all_remarks, sys_info, func_asm_stats):
    """Generate Markdown report."""
    lines = []

    lines.append("# mskql SIMD & ILP Analysis Report")
    lines.append("")
    lines.append(f"**Generated:** {sys_info.get('date', 'unknown')}")
    lines.append(f"**CPU:** {sys_info.get('cpu', 'unknown')}")
    lines.append(f"**Architecture:** {sys_info.get('arch', 'unknown')}")
    lines.append(f"**Compiler:** {sys_info.get('compiler', 'unknown')}")
    lines.append(f"**Build flags:** `-O3 -flto -march=native -ffast-math -funroll-loops`")
    lines.append("")

    # ── Executive summary ────────────────────────────────────────
    lines.append("## Executive Summary")
    lines.append("")

    total_funcs = len(grades)
    vectorized_yes = sum(1 for g in grades if g["vectorized"] == "yes")
    vectorized_partial = sum(1 for g in grades if g["vectorized"] == "partial")
    vectorized_no = sum(1 for g in grades if g["vectorized"] == "no")
    vectorized_unknown = sum(1 for g in grades if g["vectorized"] == "unknown")

    has_asm = sum(1 for g in grades if g["total_insns"] > 0)
    total_simd = sum(g["simd_insns"] for g in grades)
    total_insns = sum(g["total_insns"] for g in grades)
    overall_simd_pct = (total_simd / total_insns * 100) if total_insns > 0 else 0

    lines.append(f"Analyzed **{total_funcs}** hot-path functions across the plan executor, "
                 f"sort engine, hash join/aggregation, and wire serializer.")
    lines.append("")

    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Functions with assembly data | {has_asm}/{total_funcs} |")
    lines.append(f"| Fully vectorized (compiler remarks) | {vectorized_yes}/{total_funcs} |")
    lines.append(f"| Partially vectorized | {vectorized_partial}/{total_funcs} |")
    lines.append(f"| Not vectorized | {vectorized_no}/{total_funcs} |")
    lines.append(f"| No remark data | {vectorized_unknown}/{total_funcs} |")
    lines.append(f"| Overall SIMD instruction ratio | {overall_simd_pct:.1f}% |")
    lines.append(f"| Total instructions analyzed | {total_insns:,} |")
    lines.append(f"| Total SIMD instructions | {total_simd:,} |")
    lines.append("")

    # ILP summary
    ilp_high = sum(1 for g in grades if g["ilp_estimate"] == "high")
    ilp_mod = sum(1 for g in grades if g["ilp_estimate"] == "moderate")
    ilp_low = sum(1 for g in grades if g["ilp_estimate"] == "low")
    lines.append(f"**ILP estimate:** {ilp_high} high, {ilp_mod} moderate, {ilp_low} low")
    lines.append("")

    # Overall assessment
    if overall_simd_pct > 20:
        lines.append("> ✅ **Good SIMD utilization overall.** The columnar block design is paying off — "
                     "the compiler is generating vector instructions for many inner loops.")
    elif overall_simd_pct > 8:
        lines.append("> ⚠️ **Moderate SIMD utilization.** Some loops are vectorized but key hot paths "
                     "are not benefiting. The columnar layout enables vectorization but code patterns "
                     "are blocking it in several places.")
    else:
        lines.append("> ❌ **Low SIMD utilization.** Despite the columnar block design, the compiler "
                     "is not generating many SIMD instructions. The main blockers are indirect indexing, "
                     "branch-heavy control flow, and function pointer indirection.")
    lines.append("")

    # ── Per-function scorecard ───────────────────────────────────
    lines.append("## Per-Function Scorecard")
    lines.append("")
    lines.append("| Function | Category | Vec? | SIMD% | Branch% | ILP | Insns | Remarks |")
    lines.append("|----------|----------|------|-------|---------|-----|-------|---------|")

    for g in sorted(grades, key=lambda x: (-x["simd_pct"], x["name"])):
        vec_icon = {"yes": "✅", "partial": "⚠️", "no": "❌", "unknown": "—"}[g["vectorized"]]
        ilp_icon = {"high": "🟢", "moderate": "🟡", "low": "🔴", "unknown": "—"}[g["ilp_estimate"]]
        insns_str = f"{g['total_insns']:,}" if g["total_insns"] > 0 else "—"
        simd_str = f"{g['simd_pct']:.1f}%" if g["total_insns"] > 0 else "—"
        branch_str = f"{g['branch_pct']:.1f}%" if g["total_insns"] > 0 else "—"
        lines.append(
            f"| `{g['name']}` | {g['category']} | {vec_icon} | {simd_str} | "
            f"{branch_str} | {ilp_icon} | {insns_str} | {g['remark_summary']} |"
        )

    lines.append("")

    # ── Detailed findings per category ───────────────────────────
    lines.append("## Detailed Findings")
    lines.append("")

    categories = ["Filter", "Sort", "Join", "Aggregation", "Projection",
                  "Window", "Scan", "Generate", "Wire", "Distinct"]
    for cat in categories:
        cat_grades = [g for g in grades if g["category"] == cat]
        if not cat_grades:
            continue

        lines.append(f"### {cat}")
        lines.append("")

        for g in cat_grades:
            lines.append(f"#### `{g['name']}` — {g['description']}")
            lines.append("")

            if g["total_insns"] > 0:
                lines.append(f"- **Total instructions:** {g['total_insns']:,}")
                lines.append(f"- **SIMD instructions:** {g['simd_insns']:,} ({g['simd_pct']:.1f}%)")
                lines.append(f"- **Branch instructions:** {g['branch_insns']:,} ({g['branch_pct']:.1f}%)")
                lines.append(f"- **Load/store:** {g['load_store_pct']:.1f}%")
                if g["has_prefetch"]:
                    lines.append(f"- **Prefetch instructions:** {g['prefetch_insns']}")
                if g["neon_widths"]:
                    widths_str = ", ".join(f"{w}: {c}" for w, c in
                                          sorted(g["neon_widths"].items(), key=lambda x: -x[1]))
                    lines.append(f"- **NEON arrangement specifiers:** {widths_str}")
            else:
                lines.append("- *(no assembly data — function may be inlined or not found)*")

            lines.append(f"- **Vectorized (compiler):** {g['vectorized']} — {g['remark_summary']}")
            lines.append(f"- **ILP estimate:** {g['ilp_estimate']}")

            if g["limiting_factors"]:
                lines.append("- **Limiting factors:**")
                for factor in g["limiting_factors"]:
                    lines.append(f"  - {factor}")

            # Add SIMD instruction breakdown if present
            if g["total_insns"] > 0 and func_asm_stats.get(g["name"]):
                stats = func_asm_stats[g["name"]]
                simd_details = stats.get("simd_details", {})
                if simd_details:
                    top_simd = sorted(simd_details.items(), key=lambda x: -x[1])[:8]
                    details_str = ", ".join(f"`{m}` ×{c}" for m, c in top_simd)
                    lines.append(f"- **Top SIMD mnemonics:** {details_str}")

            lines.append("")

    # ── Recommendations ──────────────────────────────────────────
    lines.append("## Recommendations")
    lines.append("")
    lines.append("Ranked by estimated impact × feasibility:")
    lines.append("")

    recommendations = [
        {
            "title": "Split `cand[]` indirection in filter loops",
            "impact": "HIGH",
            "effort": "Low",
            "detail": (
                "The `VEC_FILTER_FUNC` macro always indexes through `cand[i]`, even when the "
                "candidate set is the identity (0..count-1). Adding a fast path where `cand == NULL` "
                "means \"all rows\" would let the compiler emit contiguous SIMD loads instead of "
                "gathers. This affects every WHERE clause."
            ),
        },
        {
            "title": "Add `__attribute__((aligned(64)))` to `col_block` arrays",
            "impact": "MEDIUM",
            "effort": "Low",
            "detail": (
                "The `nulls[]`, `i32[]`, `i64[]`, `f64[]` arrays in `struct col_block` have no "
                "alignment guarantee. Adding `alignas(64)` (or `__attribute__((aligned(64)))`) "
                "enables the compiler to use aligned SIMD loads/stores, which are faster and "
                "avoid split-line penalties."
            ),
        },
        {
            "title": "Add `restrict` qualifiers to filter/project inner loop pointers",
            "impact": "MEDIUM",
            "effort": "Low",
            "detail": (
                "The compiler may not vectorize loops where source and destination pointers could "
                "alias. Adding `restrict` to the `vals`, `nulls`, `sel`, `dst` pointers in "
                "VEC_FILTER_FUNC and vec_project_next inner loops would remove this barrier."
            ),
        },
        {
            "title": "Explicit NEON intrinsics for i32 filter (ARM64)",
            "impact": "HIGH",
            "effort": "Medium",
            "detail": (
                "The vec_filter_i32 comparison loop is the single most critical inner loop. "
                "An explicit NEON implementation using `vld1q_s32`, `vcgtq_s32`, `vst1q_u32` etc. "
                "would process 4 int32s per cycle (128-bit NEON) with guaranteed vectorization. "
                "This is a ~4x throughput improvement for the filter hot path."
            ),
        },
        {
            "title": "SIMD-accelerated radix sort histogram",
            "impact": "MEDIUM",
            "effort": "Medium",
            "detail": (
                "The histogram-building pass in radix sort (`counts[(key >> shift) & 0xFF]++`) "
                "is inherently serial due to the scatter update. A well-known technique is to "
                "use multiple histogram arrays (one per SIMD lane) and merge, or to use "
                "conflict-detection instructions (AVX-512 on x86)."
            ),
        },
        {
            "title": "Replace qsort function pointer with inline comparator",
            "impact": "MEDIUM",
            "effort": "Medium",
            "detail": (
                "The pdqsort/qsort comparators are called through function pointers, which "
                "prevents the compiler from inlining comparisons across iterations. Using a "
                "macro-generated sort (like the Linux kernel's sort or a template-style "
                "approach) would allow full inlining and SIMD-friendly comparison sequences."
            ),
        },
        {
            "title": "Branchless hash table probing",
            "impact": "MEDIUM",
            "effort": "High",
            "detail": (
                "The open-addressing `while (ht->entries[slot])` probe chains in hash_join, "
                "hash_agg, and distinct are branch-heavy and cause pipeline stalls. Switching "
                "to a Swiss Table-style layout (SIMD metadata probe) or Robin Hood hashing "
                "with branchless probing would improve ILP significantly."
            ),
        },
        {
            "title": "Batched SIMD int-to-text conversion",
            "impact": "MEDIUM",
            "effort": "High",
            "detail": (
                "The fast_i32_to_str function uses a division chain that is inherently serial. "
                "Research techniques (e.g., Lemire's fast int-to-string, or SIMD-based "
                "approaches using multiplication by magic constants) can process multiple "
                "integers in parallel."
            ),
        },
    ]

    for i, rec in enumerate(recommendations, 1):
        impact_icon = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}[rec["impact"]]
        lines.append(f"### {i}. {rec['title']}")
        lines.append("")
        lines.append(f"**Impact:** {impact_icon} {rec['impact']} · **Effort:** {rec['effort']}")
        lines.append("")
        lines.append(rec["detail"])
        lines.append("")

    # ── Appendix: All vectorization remarks ──────────────────────
    lines.append("## Appendix: Compiler Vectorization Remarks")
    lines.append("")

    total_v = 0
    total_m = 0
    for src_file, file_remarks in sorted(all_remarks.items()):
        v = sum(1 for r in file_remarks if r["kind"] == "vectorized")
        m = sum(1 for r in file_remarks if r["kind"] == "missed")
        total_v += v
        total_m += m

    lines.append(f"**Total:** {total_v} vectorized, {total_m} missed")
    lines.append("")

    for src_file in sorted(all_remarks.keys()):
        file_remarks = all_remarks[src_file]
        if not file_remarks:
            continue

        lines.append(f"### {src_file}")
        lines.append("")

        vectorized = [r for r in file_remarks if r["kind"] == "vectorized"]
        missed = [r for r in file_remarks if r["kind"] == "missed"]

        if vectorized:
            lines.append(f"**Vectorized ({len(vectorized)}):**")
            for r in sorted(vectorized, key=lambda x: x["line"]):
                func_tag = f" [{r['function']}]" if r.get("function") else ""
                lines.append(f"- Line {r['line']}: {r['message']}{func_tag}")
            lines.append("")

        if missed:
            lines.append(f"**Not vectorized ({len(missed)}):**")
            for r in sorted(missed, key=lambda x: x["line"]):
                func_tag = f" [{r['function']}]" if r.get("function") else ""
                lines.append(f"- Line {r['line']}: {r['message']}{func_tag}")
            lines.append("")

    lines.append("---")
    lines.append(f"*Report generated by `bench/parse_simd.py` on {sys_info.get('date', 'unknown')}*")
    lines.append("")
    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Analyze SIMD vectorization and ILP in mskql hot paths"
    )
    parser.add_argument("--remarks", required=True,
                        help="Path to vectorization_remarks.txt")
    parser.add_argument("--asm-dir",
                        help="Directory containing .s assembly files")
    parser.add_argument("--output", default="report.md",
                        help="Output Markdown file (default: report.md)")
    args = parser.parse_args()

    sys_info = get_system_info()
    print(f"  Architecture: {sys_info['arch']}")
    print(f"  CPU: {sys_info['cpu']}")

    # Parse vectorization remarks
    print("  Parsing vectorization remarks...")
    all_remarks = parse_remarks(args.remarks)
    total_remarks = sum(len(v) for v in all_remarks.values())
    print(f"  Found {total_remarks} remarks across {len(all_remarks)} files")

    # Get function line ranges for remark matching
    src_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
    func_line_ranges = get_func_line_ranges(src_dir)
    func_remarks = match_remarks_to_functions(all_remarks, func_line_ranges)

    # Parse assembly
    func_asm_stats = {}
    if args.asm_dir and os.path.isdir(args.asm_dir):
        print("  Parsing assembly files...")
        for asm_file in os.listdir(args.asm_dir):
            if asm_file.endswith(".s"):
                asm_path = os.path.join(args.asm_dir, asm_file)
                funcs = extract_functions_from_asm(asm_path)
                func_asm_stats.update(funcs)
                print(f"    {asm_file}: found {len(funcs)} target functions")
    else:
        print("  No assembly directory provided — remarks-only analysis")

    # Grade each function
    print("  Grading functions...")
    grades = []
    for func_name in TARGET_FUNCTIONS:
        asm_stats = func_asm_stats.get(func_name)
        fr = func_remarks.get(func_name, [])
        grade = grade_function(func_name, asm_stats, fr)
        grades.append(grade)

    # Generate report
    print("  Generating report...")
    report = generate_report(grades, all_remarks, sys_info, func_asm_stats)

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"  Report written to: {args.output}")

    # Print summary to console
    print("")
    has_asm = sum(1 for g in grades if g["total_insns"] > 0)
    total_simd = sum(g["simd_insns"] for g in grades)
    total_insns = sum(g["total_insns"] for g in grades)
    simd_pct = (total_simd / total_insns * 100) if total_insns > 0 else 0
    vec_yes = sum(1 for g in grades if g["vectorized"] == "yes")
    vec_partial = sum(1 for g in grades if g["vectorized"] == "partial")
    vec_no = sum(1 for g in grades if g["vectorized"] == "no")

    print(f"  ┌─────────────────────────────────────┐")
    print(f"  │ Functions analyzed:  {len(grades):>3d}              │")
    print(f"  │ With assembly data: {has_asm:>3d}              │")
    print(f"  │ SIMD ratio:         {simd_pct:>5.1f}%           │")
    print(f"  │ Vectorized:         {vec_yes:>3d} yes, {vec_partial} partial │")
    print(f"  │ Not vectorized:     {vec_no:>3d}              │")
    print(f"  └─────────────────────────────────────┘")


if __name__ == "__main__":
    main()
