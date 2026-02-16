# Auto-detect Homebrew LLVM clang on macOS (user can override with CC=...)
ifeq ($(origin CC),default)
  UNAME_S := $(shell uname -s)
  ifeq ($(UNAME_S),Darwin)
    HOMEBREW_CLANG := /opt/homebrew/opt/llvm/bin/clang
    ifeq ($(wildcard $(HOMEBREW_CLANG)),)
      $(error Homebrew LLVM clang not found at $(HOMEBREW_CLANG). Install with: brew install llvm)
    endif
    CC := $(HOMEBREW_CLANG)
  endif
endif
CFLAGS  ?= -Wall -Wextra -Wswitch-enum -Wpedantic -std=c11 -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE -O1 -g -fsanitize=address -fno-omit-frame-pointer
BUILDDIR = ../build
SRCS     = main.c database.c table.c query.c row.c parser.c pgwire.c index.c column.c plan.c catalog.c datetime.c
OBJS     = $(patsubst %.c,$(BUILDDIR)/%.o,$(SRCS))
LIB_SRCS = database.c table.c query.c row.c parser.c index.c column.c plan.c catalog.c datetime.c
LIB_OBJS = $(patsubst %.c,$(BUILDDIR)/%.o,$(LIB_SRCS))
TARGET   = $(BUILDDIR)/mskql_debug
BENCH    = $(BUILDDIR)/mskql_bench

# ── Release flags (for benchmarks / production) ──────────────────
RELEASE_CFLAGS = -Wall -Wextra -Wswitch-enum -Wpedantic -std=c11 -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE -O3 -flto -DNDEBUG -g
RELEASE_OBJS     = $(patsubst %.c,$(BUILDDIR)/rel_%.o,$(SRCS))
RELEASE_LIB_OBJS = $(patsubst %.c,$(BUILDDIR)/rel_%.o,$(LIB_SRCS))

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ -lm

RELEASE_TARGET = $(BUILDDIR)/mskql

release: $(RELEASE_OBJS)
	$(CC) $(RELEASE_CFLAGS) -o $(RELEASE_TARGET) $^ -lm

bench: $(RELEASE_LIB_OBJS) $(BUILDDIR)/rel_bench.o
	$(CC) $(RELEASE_CFLAGS) -o $(BENCH) $^ -lm

bench-throughput: | $(BUILDDIR)
	$(CC) -Wall -Wextra -Wpedantic -std=c11 -D_POSIX_C_SOURCE=200809L -O2 -g -pthread -o $(BUILDDIR)/mskql_bench_tp ../bench/bench_throughput.c

$(BUILDDIR)/rel_bench.o: ../bench/bench.c | $(BUILDDIR)
	$(CC) $(RELEASE_CFLAGS) -c -o $@ $<

$(BUILDDIR)/rel_%.o: %.c | $(BUILDDIR)
	$(CC) $(RELEASE_CFLAGS) -c -o $@ $<

$(BUILDDIR)/%.o: %.c | $(BUILDDIR)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

clean:
	rm -rf $(BUILDDIR)

# ── WASM freestanding build ──────────────────────────────────────
WASM_CC     ?= clang
WASM_INCDIR  = $(BUILDDIR)/wasm-include
WASM_CFLAGS  = --target=wasm32-unknown-unknown -ffreestanding -nostdlib -nostdinc \
               -isystem $(WASM_INCDIR) \
               -std=c11 -O2 -flto -DNDEBUG -DMSKQL_WASM \
               -include wasm_libc.h \
               -Wno-builtin-requires-header
WASM_LDFLAGS = -Wl,--no-entry \
               -Wl,--allow-undefined \
               -Wl,--export=mskql_open \
               -Wl,--export=mskql_exec \
               -Wl,--export=mskql_close \
               -Wl,--export=mskql_alloc \
               -Wl,--export=mskql_dealloc \
               -Wl,--initial-memory=16777216 \
               -Wl,--max-memory=268435456
WASM_SRCS    = wasm_api.c wasm_libc.c database.c table.c query.c row.c \
               parser.c index.c column.c plan.c catalog.c datetime.c
WASM_TARGET  = $(BUILDDIR)/mskql.wasm

wasm: wasm-stubs $(WASM_TARGET)

wasm-stubs: | $(BUILDDIR)
	@mkdir -p $(WASM_INCDIR)
	@for h in stdio.h stdlib.h string.h strings.h stddef.h stdint.h stdbool.h stdarg.h ctype.h math.h time.h signal.h; do \
		echo '/* stub */' > $(WASM_INCDIR)/$$h; \
	done

$(WASM_TARGET): $(WASM_SRCS) | $(BUILDDIR)
	$(WASM_CC) $(WASM_CFLAGS) $(WASM_LDFLAGS) -o $@ $(WASM_SRCS)

.PHONY: all clean release bench bench-throughput wasm
