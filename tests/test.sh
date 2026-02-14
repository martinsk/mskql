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
# Each testcase file is run in isolation (database is reset between tests).
#
# Tests run in parallel across all available CPU cores.
# Each worker gets a unique port (base 15433 + worker index) and a
# persistent server process that is reused across all tests in that worker.
# Between tests the database is reset via SELECT __reset_db().
#
# The default build uses ASAN (AddressSanitizer + LeakSanitizer).
# After all tests the server's ASAN log is checked; leaks cause failure.
# Set MSKQL_NO_LEAK_CHECK=1 to skip leak checking.
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TESTCASE_DIR="$(cd "$SCRIPT_DIR/cases" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BASE_PORT=${MSKQL_TEST_BASE_PORT:-15433}
NO_LEAK_CHECK=${MSKQL_NO_LEAK_CHECK:-0}
LSAN_SUPP="$SCRIPT_DIR/lsan_suppressions.txt"
FAILURE_LOG="$SCRIPT_DIR/test-failures.log"

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

# Wait until a port is free, killing any occupying mskql process.
# Usage: wait_for_port_free <port>
wait_for_port_free() {
    local port="$1"
    local max_attempts=20
    for ((attempt = 0; attempt < max_attempts; attempt++)); do
        # check if anything is listening on this port
        local pids
        pids=$(lsof -ti :"$port" 2>/dev/null || true)
        if [ -z "$pids" ]; then
            return 0  # port is free
        fi
        if [ "$attempt" -eq 0 ]; then
            # first attempt: try to kill occupying mskql processes
            for p in $pids; do
                local pname
                pname=$(ps -p "$p" -o comm= 2>/dev/null || true)
                if [[ "$pname" == *mskql* ]]; then
                    kill -TERM "$p" 2>/dev/null || true
                fi
            done
        fi
        sleep 0.25
    done
    # port still occupied after timeout
    echo "WARNING: port $port still in use after ${max_attempts} attempts" >&2
    return 1
}

# Kill any stale mskql processes on ports we plan to use.
kill_stale_servers() {
    local ports_to_check=()
    for ((i = 0; i < NWORKERS; i++)); do
        ports_to_check+=($((BASE_PORT + i)))
    done
    # C test suite ports
    ports_to_check+=(15400 15401)

    for port in "${ports_to_check[@]}"; do
        local pids
        pids=$(lsof -ti :"$port" 2>/dev/null || true)
        if [ -n "$pids" ]; then
            for p in $pids; do
                local pname
                pname=$(ps -p "$p" -o comm= 2>/dev/null || true)
                if [[ "$pname" == *mskql* ]]; then
                    kill -TERM "$p" 2>/dev/null || true
                fi
            done
        fi
    done
    # brief wait for killed processes to release sockets
    sleep 0.5
}

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
    # Ensure port is free before starting
    if ! wait_for_port_free "$port"; then
        echo "ERROR: cannot start server — port $port is not free" >&2
        return 1
    fi
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
        echo "ERROR: server on port $port (pid $srv_pid) died during startup" >&2
        return 1
    fi
    echo "WARNING: server on port $port (pid $srv_pid) running but not accepting connections after 5s" >&2
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
        # brief pause for socket TIME_WAIT to clear
        sleep 0.1
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

# Ensure the server for this worker is running. Starts or restarts as needed.
# Sets SERVER_OK=1 on success, 0 on failure.
ensure_server() {
    local port="$1"
    local pidfile="$2"
    local worker_dir="$3"
    SERVER_OK=1
    if [ -f "$pidfile" ]; then
        local pid
        pid=$(<"$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            return  # already running
        fi
        # server died — restart
        rm -f "$pidfile"
    fi
    if ! start_server "$port" "$pidfile" "$worker_dir"; then
        SERVER_OK=0
    fi
}

# Run a batch of tests on a single worker (persistent server).
# Reads test file paths from a manifest file (one per line).
# Writes per-test results to individual result files.
run_worker_batch() {
    local worker_id="$1"
    local manifest="$2"
    local port=$((BASE_PORT + worker_id))
    local worker_dir="$TMPDIR_ROOT/worker-$worker_id"
    local pidfile="$worker_dir/server.pid"

    mkdir -p "$worker_dir"

    # start the persistent server for this worker
    if ! start_server "$port" "$pidfile" "$worker_dir"; then
        # mark all tests in this batch as failed
        while IFS= read -r file; do
            local rf="$TMPDIR_ROOT/result-$(basename "$file" .sql).txt"
            local relpath="${file#$PROJECT_DIR/}"
            { echo "FAIL"; echo "FAIL $relpath: (server failed to start)"; } > "$rf"
        done < "$manifest"
        return
    fi

    while IFS= read -r file; do
        [ -z "$file" ] && continue
        local rf="$TMPDIR_ROOT/result-$(basename "$file" .sql).txt"
        local relpath="${file#$PROJECT_DIR/}"

        parse_testcase "$file"

        # reset database to clean state (instead of restarting server)
        if ! psql_cmd "$port" -c "SELECT __reset_db()" >/dev/null 2>&1; then
            # server may have crashed — try to restart
            stop_server "$pidfile"
            if ! start_server "$port" "$pidfile" "$worker_dir"; then
                { echo "FAIL"; echo "FAIL $relpath: $TC_NAME (server crashed and failed to restart)"; } > "$rf"
                continue
            fi
        fi

        # run setup SQL in a single psql session
        # (ensure each line ends with a semicolon so psql can parse them)
        if [ -n "$TC_SETUP" ]; then
            echo "$TC_SETUP" | sed 's/;*$/;/' | psql_cmd "$port" >/dev/null 2>&1
        fi

        # run input SQL in a single psql session
        local actual="" status=0
        if [ -n "$TC_INPUT" ]; then
            actual=$(echo "$TC_INPUT" | sed 's/;*$/;/' | psql_cmd "$port" 2>&1) || status=$?
        fi

        # compare
        local ok=1
        local fail_details=""

        if [ "$TC_STATUS" = "0" ] && [ "$status" -ne 0 ]; then
            ok=0
            fail_details="${fail_details}    expected status 0, got $status"$'\n'
        fi

        if [ -n "$TC_EXPECTED" ]; then
            if [ "$TC_EXPECTED" != "$actual" ]; then
                ok=0
                fail_details="${fail_details}    expected output:"$'\n'
                fail_details="${fail_details}$(echo "$TC_EXPECTED" | sed 's/^/      /')"$'\n'
                fail_details="${fail_details}    actual output:"$'\n'
                fail_details="${fail_details}$(echo "$actual" | sed 's/^/      /')"$'\n'
            fi
        fi

        if [ "$ok" -eq 1 ]; then
            echo "PASS" > "$rf"
        else
            { echo "FAIL"; echo "FAIL $relpath: $TC_NAME"; printf '%s' "$fail_details"; } > "$rf"
        fi
    done < "$manifest"

    # stop the persistent server
    stop_server "$pidfile"

    # check for memory leaks (ASAN) — once at the end for this worker
    if [ "$NO_LEAK_CHECK" != "1" ]; then
        local leak_info
        leak_info=$(check_asan_logs "$worker_dir")
        if [ -n "$leak_info" ]; then
            # write a special result file for the ASAN failure
            local rf="$TMPDIR_ROOT/result-asan-worker-$worker_id.txt"
            { echo "FAIL"; echo "FAIL worker-$worker_id: memory leak detected"; echo "    $leak_info"; } > "$rf"
        fi
    fi
}

# ---- build ----
# Force a clean rebuild if the compiler changed (avoids ASAN version mismatches)
_CC="${CC:-cc}"
_CC_STAMP="$PROJECT_DIR/build/.cc_stamp"
_CC_ID=$("$_CC" --version 2>&1 | head -1)
if [ -f "$_CC_STAMP" ] && [ "$(<"$_CC_STAMP")" != "$_CC_ID" ]; then
    (cd "$PROJECT_DIR/src" && make clean) >/dev/null 2>&1
fi
if ! BUILD_OUT=$(cd "$PROJECT_DIR/src" && make CC="$_CC" 2>&1); then
    echo "BUILD FAILED:"
    echo "$BUILD_OUT"
    exit 1
fi
mkdir -p "$PROJECT_DIR/build"
echo "$_CC_ID" > "$_CC_STAMP"

# ---- kill stale servers from previous runs ----
kill_stale_servers

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

# ---- partition tests across workers and run in parallel ----
# Create manifest files (one per worker) with test file paths.
for ((i = 0; i < NWORKERS; i++)); do
    mkdir -p "$TMPDIR_ROOT/worker-$i"
    : > "$TMPDIR_ROOT/worker-$i/manifest.txt"
done

idx=0
for f in "${testfiles[@]}"; do
    worker=$((idx % NWORKERS))
    echo "$f" >> "$TMPDIR_ROOT/worker-$worker/manifest.txt"
    idx=$((idx + 1))
done

# launch all workers in parallel
worker_pids=()
for ((i = 0; i < NWORKERS; i++)); do
    manifest="$TMPDIR_ROOT/worker-$i/manifest.txt"
    if [ -s "$manifest" ]; then
        run_worker_batch "$i" "$manifest" &
        worker_pids+=($!)
    fi
done

# wait for all workers, showing progress
PASS=0
FAIL=0
FAIL_OUTPUT=""
DONE=0

show_progress() {
    printf '\r[%d/%d]' "$DONE" "$total" >&2
}

show_progress

# poll for completed result files while workers are running
all_done=0
while [ "$all_done" -eq 0 ]; do
    all_done=1
    for pid in "${worker_pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            all_done=0
        fi
    done

    # count completed results
    local_done=0
    for f in "${testfiles[@]}"; do
        rf="$TMPDIR_ROOT/result-$(basename "$f" .sql).txt"
        if [ -f "$rf" ]; then
            local_done=$((local_done + 1))
        fi
    done
    if [ "$local_done" -ne "$DONE" ]; then
        DONE=$local_done
        show_progress
    fi

    [ "$all_done" -eq 0 ] && sleep 0.1
done

# wait for all workers to fully exit
for pid in "${worker_pids[@]}"; do
    wait "$pid" 2>/dev/null || true
done

# collect all results
for f in "${testfiles[@]}"; do
    rf="$TMPDIR_ROOT/result-$(basename "$f" .sql).txt"
    if [ -f "$rf" ]; then
        first=$(head -1 "$rf")
        if [ "$first" = "PASS" ]; then
            PASS=$((PASS + 1))
        else
            FAIL=$((FAIL + 1))
            details=$(tail -n +2 "$rf")
            if [ -n "$details" ]; then
                FAIL_OUTPUT="${FAIL_OUTPUT}${details}"$'\n'
            fi
        fi
        rm -f "$rf"
    fi
done

# collect ASAN worker-level results
for ((i = 0; i < NWORKERS; i++)); do
    rf="$TMPDIR_ROOT/result-asan-worker-$i.txt"
    if [ -f "$rf" ]; then
        FAIL=$((FAIL + 1))
        details=$(tail -n +2 "$rf")
        if [ -n "$details" ]; then
            FAIL_OUTPUT="${FAIL_OUTPUT}${details}"$'\n'
        fi
        rm -f "$rf"
    fi
done

DONE=$total
show_progress

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
    # map known C test suite names to their default ports
    ctest_port=""
    case "$ctest_name" in
        concurrent) ctest_port=15400 ;;
        extended)   ctest_port=15401 ;;
    esac
    # ensure the port for this C test suite is free
    if [ -n "$ctest_port" ]; then
        if ! wait_for_port_free "$ctest_port"; then
            FAIL=$((FAIL + 1))
            FAIL_OUTPUT="${FAIL_OUTPUT}FAIL tests/cases/$ctest_name/: port $ctest_port not free"$'\n'
            continue
        fi
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

# ---- write failure log ----
if [ -n "$FAIL_OUTPUT" ]; then
    {
        echo "========================================"
        echo "mskql test failure log"
        echo "Date: $(date '+%Y-%m-%d %H:%M:%S %Z')"
        echo "Tests: $PASS passed, $FAIL failed (of $((PASS + FAIL)) total)"
        echo "========================================"
        echo ""
        printf '%s' "$FAIL_OUTPUT"
    } > "$FAILURE_LOG"
fi

# ---- summary ----
if [ -n "$FAIL_OUTPUT" ]; then
    printf '%s' "$FAIL_OUTPUT"
fi

if [ "$FAIL" -gt 0 ]; then
    echo "$PASS passed, $FAIL failed"
    echo "Failure details written to: $FAILURE_LOG" >&2
    exit 1
else
    # clean up stale log from a previous failing run
    rm -f "$FAILURE_LOG"
    echo "All $PASS tests passed"
    exit 0
fi
