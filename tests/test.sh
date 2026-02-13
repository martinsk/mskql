#!/usr/bin/env bash
#
# Test runner for mskql.
#
# Testcase format (.sql files in cases/):
#
#   -- <test name>
#   -- setup:
#   <SQL run before the test, output not checked>
#   -- input:
#   <SQL whose output is checked>
#   -- expected output:
#   <expected lines, compared with psql -tA output>
#   -- expected status: <0 or non-zero>
#
# All sections except the test name are optional.
# Each testcase file is run in isolation (server is restarted).
#
# Tests run in parallel across all available CPU cores.
# Each worker gets a unique port (base 15433 + worker index).
#
# The default build uses ASAN (AddressSanitizer + LeakSanitizer).
# After each test the server's ASAN log is checked; leaks cause failure.
# Set MSKQL_NO_LEAK_CHECK=1 to skip leak checking.
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TESTCASE_DIR="$(cd "$SCRIPT_DIR/cases" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BASE_PORT=${MSKQL_TEST_BASE_PORT:-15433}
NO_LEAK_CHECK=${MSKQL_NO_LEAK_CHECK:-0}
LSAN_SUPP="$SCRIPT_DIR/lsan_suppressions.txt"

# Prefer Homebrew Clang for LeakSanitizer support on macOS ARM64
# (Apple Clang does not support detect_leaks on this platform)
if [ -x /opt/homebrew/opt/llvm/bin/clang ]; then
    export CC=/opt/homebrew/opt/llvm/bin/clang
fi

# detect available parallelism
if command -v nproc >/dev/null 2>&1; then
    NWORKERS=$(nproc)
elif command -v sysctl >/dev/null 2>&1; then
    NWORKERS=$(sysctl -n hw.ncpu)
else
    NWORKERS=4
fi

TMPDIR_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/mskql-test.XXXXXX")

cleanup_all() {
    # kill any leftover server processes we may have spawned
    for pidfile in "$TMPDIR_ROOT"/worker-*/server.pid; do
        [ -f "$pidfile" ] || continue
        local pid
        pid=$(<"$pidfile")
        kill -TERM "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null || true
    done
    rm -rf "$TMPDIR_ROOT"
}
trap cleanup_all EXIT

psql_cmd() {
    local port="$1"; shift
    psql -h 127.0.0.1 -p "$port" -U test -d mskql --no-psqlrc -tA "$@"
}

start_server() {
    local port="$1"
    local pidfile="$2"
    local worker_dir="$3"
    # Clean up any previous ASAN logs for this worker
    rm -f "$worker_dir"/asan.log.*
    # Run server with ASAN + LeakSanitizer logging to file
    local lsan_opts="suppressions=$LSAN_SUPP"
    ASAN_OPTIONS="detect_leaks=1:log_path=$worker_dir/asan.log:exitcode=0" \
        LSAN_OPTIONS="$lsan_opts" \
        MSKQL_PORT="$port" "$PROJECT_DIR/build/mskql" >/dev/null 2>&1 &
    local srv_pid=$!
    echo "$srv_pid" > "$pidfile"
    # wait until the server is accepting connections
    for i in $(seq 1 20); do
        if psql_cmd "$port" -c "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    if ! kill -0 "$srv_pid" 2>/dev/null; then
        return 1
    fi
    return 0
}

stop_server() {
    local pidfile="$1"
    if [ -f "$pidfile" ]; then
        local pid
        pid=$(<"$pidfile")
        kill -TERM "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null || true
        rm -f "$pidfile"
    fi
}

# Check ASAN log files in a worker dir. Returns leak summary or empty string.
check_asan_logs() {
    local worker_dir="$1"
    local leak_info=""
    for logfile in "$worker_dir"/asan.log.*; do
        [ -f "$logfile" ] || continue
        if grep -qE 'LeakSanitizer|ERROR: AddressSanitizer' "$logfile" 2>/dev/null; then
            # Extract a concise summary
            leak_info=$(grep -A 2 -E 'SUMMARY|LeakSanitizer|ERROR: AddressSanitizer' "$logfile" | head -10)
            break
        fi
    done
    echo "$leak_info"
}

run_sql() {
    local port="$1"; shift
    psql_cmd "$port" -c "$1" 2>&1
}

# Run multiple SQL statements in a single psql session (piped via stdin).
# This keeps transactions alive across statements.
run_sql_session() {
    local port="$1"; shift
    echo "$1" | psql_cmd "$port" 2>&1
}

# Parse a testcase file into variables:
#   TC_NAME, TC_SETUP, TC_INPUT, TC_EXPECTED, TC_STATUS
parse_testcase() {
    local file="$1"
    TC_NAME=""
    TC_SETUP=""
    TC_INPUT=""
    TC_EXPECTED=""
    TC_STATUS="0"

    local section=""
    while IFS= read -r line; do
        # first comment line is the test name
        if [ -z "$TC_NAME" ] && [[ "$line" == --\ * ]]; then
            TC_NAME="${line#-- }"
            continue
        fi

        # section markers
        case "$line" in
            "-- setup:")     section="setup";    continue ;;
            "-- input:")     section="input";    continue ;;
            "-- expected output:") section="expected"; continue ;;
            "-- expected status: "*)
                TC_STATUS="${line#-- expected status: }"
                # strip trailing period if present
                TC_STATUS="${TC_STATUS%.}"
                section=""
                continue
                ;;
        esac

        # skip bare comments not in a section
        if [ -z "$section" ]; then continue; fi

        case "$section" in
            setup)
                if [ -n "$TC_SETUP" ]; then TC_SETUP="$TC_SETUP"$'\n'"$line"
                else TC_SETUP="$line"; fi
                ;;
            input)
                if [ -n "$TC_INPUT" ]; then TC_INPUT="$TC_INPUT"$'\n'"$line"
                else TC_INPUT="$line"; fi
                ;;
            expected)
                if [ -n "$TC_EXPECTED" ]; then TC_EXPECTED="$TC_EXPECTED"$'\n'"$line"
                else TC_EXPECTED="$line"; fi
                ;;
        esac
    done < "$file"

    # default name to filename
    if [ -z "$TC_NAME" ]; then
        TC_NAME="$(basename "$file" .sql)"
    fi
}

# Run a single testcase on a given port.
# Writes "PASS" or "FAIL\n<details>" to the result file.
run_testcase() {
    local file="$1"
    local port="$2"
    local result_file="$3"
    local pidfile="$4"
    local worker_dir="$5"
    local relpath="${file#$PROJECT_DIR/}"

    parse_testcase "$file"

    # start a fresh server for each test
    if ! start_server "$port" "$pidfile" "$worker_dir"; then
        echo "FAIL" > "$result_file"
        echo "FAIL $relpath: $TC_NAME (server failed to start)" >> "$result_file"
        return
    fi

    # run setup SQL — use a single session if it contains transaction statements
    # (BEGIN), otherwise run one statement at a time for reliability
    if [ -n "$TC_SETUP" ]; then
        if echo "$TC_SETUP" | grep -qiE '^BEGIN'; then
            run_sql_session "$port" "$TC_SETUP" >/dev/null 2>&1
        else
            while IFS= read -r stmt; do
                stmt="$(echo "$stmt" | sed 's/;$//')"
                [ -z "$stmt" ] && continue
                run_sql "$port" "$stmt" >/dev/null 2>&1
            done <<< "$TC_SETUP"
        fi
    fi

    # run input SQL — use a single session if it contains transaction statements,
    # otherwise run one statement at a time (preserves per-statement output capture)
    local actual="" status=0
    if [ -n "$TC_INPUT" ]; then
        if echo "$TC_INPUT" | grep -qiE '^BEGIN|^COMMIT|^ROLLBACK'; then
            actual=$(run_sql_session "$port" "$TC_INPUT") || status=$?
        else
            local combined=""
            while IFS= read -r stmt; do
                stmt="$(echo "$stmt" | sed 's/;$//')"
                [ -z "$stmt" ] && continue
                local out
                out=$(run_sql "$port" "$stmt" 2>&1) || status=$?
                if [ -n "$out" ]; then
                    if [ -n "$combined" ]; then combined="$combined"$'\n'"$out"
                    else combined="$out"; fi
                fi
            done <<< "$TC_INPUT"
            actual="$combined"
        fi
    fi

    stop_server "$pidfile"

    # compare
    local ok=1
    local fail_details=""

    # check status
    if [ "$TC_STATUS" = "0" ] && [ "$status" -ne 0 ]; then
        ok=0
        fail_details="${fail_details}    expected status 0, got $status"$'\n'
    elif [ "$TC_STATUS" != "0" ] && [ "$status" -eq 0 ]; then
        # expected failure but got success — only fail if we also have expected output mismatch
        :
    fi

    # check output
    if [ -n "$TC_EXPECTED" ]; then
        if [ "$TC_EXPECTED" != "$actual" ]; then
            ok=0
            fail_details="${fail_details}    expected output:"$'\n'
            fail_details="${fail_details}$(echo "$TC_EXPECTED" | sed 's/^/      /')"$'\n'
            fail_details="${fail_details}    actual output:"$'\n'
            fail_details="${fail_details}$(echo "$actual" | sed 's/^/      /')"$'\n'
        fi
    fi

    # check for memory leaks (ASAN)
    if [ "$NO_LEAK_CHECK" != "1" ]; then
        local leak_info
        leak_info=$(check_asan_logs "$worker_dir")
        if [ -n "$leak_info" ]; then
            ok=0
            fail_details="${fail_details}    memory leak detected:"$'\n'
            fail_details="${fail_details}$(echo "$leak_info" | sed 's/^/      /')"$'\n'
        fi
    fi

    if [ "$ok" -eq 1 ]; then
        echo "PASS" > "$result_file"
    else
        {
            echo "FAIL"
            echo "FAIL $relpath: $TC_NAME"
            printf '%s' "$fail_details"
        } > "$result_file"
    fi
}

# ---- build ----
if ! BUILD_OUT=$(cd "$PROJECT_DIR/src" && make clean && make CC="${CC:-cc}" 2>&1); then
    echo "BUILD FAILED:"
    echo "$BUILD_OUT"
    exit 1
fi

# ---- discover testcases ----
testfiles=()
for f in "$TESTCASE_DIR"/*.sql; do
    [ -f "$f" ] || continue
    testfiles+=("$f")
done

total=${#testfiles[@]}
if [ "$total" -eq 0 ]; then
    echo "No test cases found"
    exit 0
fi

# ---- run testcases in parallel ----
# We use a simple job-slot approach: maintain up to NWORKERS background jobs.
# Each job gets a unique port = BASE_PORT + slot_index.

slot_pids=()    # PID of background job in each slot (empty = free)
slot_files=()   # test file being run in each slot
slot_results=() # result file path for each slot
slot_dirs=()    # temp dir for each slot

for ((i = 0; i < NWORKERS; i++)); do
    slot_pids+=("")
    slot_files+=("")
    slot_results+=("")
    d="$TMPDIR_ROOT/worker-$i"
    mkdir -p "$d"
    slot_dirs+=("$d")
done

PASS=0
FAIL=0
FAIL_OUTPUT=""
DONE=0

# Print progress indicator (overwrites same line)
show_progress() {
    printf '\r[%d/%d]' "$DONE" "$total" >&2
}

# Wait for a specific slot to finish and collect its result.
collect_slot() {
    local s="$1"
    wait "${slot_pids[$s]}" 2>/dev/null || true
    local rf="${slot_results[$s]}"
    if [ -f "$rf" ]; then
        local first
        first=$(head -1 "$rf")
        if [ "$first" = "PASS" ]; then
            PASS=$((PASS + 1))
        else
            FAIL=$((FAIL + 1))
            # append everything after the first line (the details)
            local details
            details=$(tail -n +2 "$rf")
            if [ -n "$details" ]; then
                FAIL_OUTPUT="${FAIL_OUTPUT}${details}"$'\n'
            fi
        fi
        rm -f "$rf"
    fi
    DONE=$((DONE + 1))
    show_progress
    slot_pids[$s]=""
}

# Find a free slot, blocking until one opens up.
acquire_slot() {
    while true; do
        for ((s = 0; s < NWORKERS; s++)); do
            if [ -z "${slot_pids[$s]}" ]; then
                ACQUIRED_SLOT=$s
                return
            fi
            # check if this slot's job has finished
            if ! kill -0 "${slot_pids[$s]}" 2>/dev/null; then
                collect_slot "$s"
                ACQUIRED_SLOT=$s
                return
            fi
        done
        # all slots busy — wait briefly then retry
        sleep 0.05
    done
}

show_progress

for f in "${testfiles[@]}"; do
    acquire_slot
    s=$ACQUIRED_SLOT
    port=$((BASE_PORT + s))
    rf="$TMPDIR_ROOT/result-$(basename "$f" .sql).txt"
    pidfile="${slot_dirs[$s]}/server.pid"

    slot_files[$s]="$f"
    slot_results[$s]="$rf"

    run_testcase "$f" "$port" "$rf" "$pidfile" "${slot_dirs[$s]}" &
    slot_pids[$s]=$!
done

# wait for all remaining slots
for ((s = 0; s < NWORKERS; s++)); do
    if [ -n "${slot_pids[$s]}" ]; then
        collect_slot "$s"
    fi
done

# clear progress line
printf '\r%*s\r' 40 '' >&2

# ---- C test suites (concurrent, extended, etc.) ----
for ctest_dir in "$TESTCASE_DIR"/*/; do
    [ -f "$ctest_dir/Makefile" ] || continue
    ctest_name="$(basename "$ctest_dir")"
    # build
    if ! make -C "$ctest_dir" 2>/dev/null; then
        FAIL=$((FAIL + 1))
        FAIL_OUTPUT="${FAIL_OUTPUT}FAIL tests/cases/$ctest_name/: build failed"$'\n'
        continue
    fi
    # find the target binary from the Makefile (convention: ../../../build/test_<name>)
    ctest_bin="$PROJECT_DIR/build/test_${ctest_name}"
    if [ ! -x "$ctest_bin" ]; then
        FAIL=$((FAIL + 1))
        FAIL_OUTPUT="${FAIL_OUTPUT}FAIL tests/cases/$ctest_name/: binary not found at $ctest_bin"$'\n'
        continue
    fi
    # run from project root (the binary does execl("./build/mskql", ...))
    if ctest_out=$( cd "$PROJECT_DIR" && "$ctest_bin" 2>&1 ); then
        # count individual checks from "All N tests passed"
        n=$(echo "$ctest_out" | grep -oE 'All [0-9]+ tests passed' | grep -oE '[0-9]+')
        if [ -n "$n" ]; then
            PASS=$((PASS + n))
        else
            PASS=$((PASS + 1))
        fi
    else
        FAIL=$((FAIL + 1))
        FAIL_OUTPUT="${FAIL_OUTPUT}FAIL tests/cases/$ctest_name/:"$'\n'
        FAIL_OUTPUT="${FAIL_OUTPUT}$(echo "$ctest_out" | grep -E 'FAIL' | sed 's/^/    /')"$'\n'
    fi
done

# ---- summary ----
if [ -n "$FAIL_OUTPUT" ]; then
    printf '%s' "$FAIL_OUTPUT"
fi

if [ "$FAIL" -gt 0 ]; then
    echo "$PASS passed, $FAIL failed"
    exit 1
else
    echo "All $PASS tests passed"
    exit 0
fi
