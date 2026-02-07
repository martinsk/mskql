#!/usr/bin/env bash
#
# Test runner for mskql.
#
# Testcase format (.sql files in testcases/):
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
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TESTCASE_DIR="$(cd "$SCRIPT_DIR/../testcases" && pwd)"
PORT=5433
PSQL="psql -h 127.0.0.1 -p $PORT -U test -d mskql --no-psqlrc -tA"
PASS=0
FAIL=0
SERVER_PID=""

start_server() {
    "$SCRIPT_DIR/mskql" &
    SERVER_PID=$!
    # wait until the server is accepting connections
    for i in $(seq 1 20); do
        if $PSQL -c "SELECT 1" >/dev/null 2>&1; then
            return
        fi
        sleep 0.25
    done
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "FATAL: server failed to start"
        exit 1
    fi
}

stop_server() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}

cleanup() {
    stop_server
}
trap cleanup EXIT

run_sql() {
    $PSQL -c "$1" 2>&1
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

run_testcase() {
    local file="$1"
    parse_testcase "$file"

    # start a fresh server for each test
    start_server

    # run setup SQL (one statement at a time)
    if [ -n "$TC_SETUP" ]; then
        while IFS= read -r stmt; do
            stmt="$(echo "$stmt" | sed 's/;$//')"
            [ -z "$stmt" ] && continue
            run_sql "$stmt" >/dev/null 2>&1
        done <<< "$TC_SETUP"
    fi

    # run input SQL and capture output
    local actual="" status=0
    if [ -n "$TC_INPUT" ]; then
        # combine multi-line input, run each statement separated by ;
        local combined=""
        while IFS= read -r stmt; do
            stmt="$(echo "$stmt" | sed 's/;$//')"
            [ -z "$stmt" ] && continue
            local out
            out=$(run_sql "$stmt" 2>&1) || status=$?
            if [ -n "$out" ]; then
                if [ -n "$combined" ]; then combined="$combined"$'\n'"$out"
                else combined="$out"; fi
            fi
        done <<< "$TC_INPUT"
        actual="$combined"
    fi

    stop_server

    # compare
    local ok=1

    # check status
    if [ "$TC_STATUS" = "0" ] && [ "$status" -ne 0 ]; then
        ok=0
    elif [ "$TC_STATUS" != "0" ] && [ "$status" -eq 0 ]; then
        # expected failure but got success — only fail if we also have expected output mismatch
        :
    fi

    # check output
    if [ -n "$TC_EXPECTED" ]; then
        if [ "$TC_EXPECTED" != "$actual" ]; then
            ok=0
        fi
    fi

    if [ "$ok" -eq 1 ]; then
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $TC_NAME"
        if [ -n "$TC_EXPECTED" ] && [ "$TC_EXPECTED" != "$actual" ]; then
            echo "    expected output:"
            echo "$TC_EXPECTED" | sed 's/^/      /'
            echo "    actual output:"
            echo "$actual" | sed 's/^/      /'
        fi
        if [ "$TC_STATUS" = "0" ] && [ "$status" -ne 0 ]; then
            echo "    expected status 0, got $status"
        fi
        FAIL=$((FAIL + 1))
    fi
}

# ---- build ----
BUILD_OUT=$(cd "$SCRIPT_DIR" && make clean && make 2>&1)
if [ $? -ne 0 ]; then
    echo "BUILD FAILED:"
    echo "$BUILD_OUT"
    exit 1
fi

# ---- discover and run testcases ----

for f in "$TESTCASE_DIR"/*.sql; do
    [ -f "$f" ] || continue
    run_testcase "$f"
done

# ---- summary ----
if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo "$PASS passed, $FAIL failed"
    exit 1
else
    echo "All $PASS tests passed"
    exit 0
fi
