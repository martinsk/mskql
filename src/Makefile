# Auto-detect Homebrew LLVM clang on macOS (user can override with CC=...)
UNAME_S := $(shell uname -s)
ifeq ($(origin CC),default)
  ifeq ($(UNAME_S),Darwin)
    HOMEBREW_CLANG := /opt/homebrew/opt/llvm/bin/clang
    ifeq ($(wildcard $(HOMEBREW_CLANG)),)
      $(error Homebrew LLVM clang not found at $(HOMEBREW_CLANG). Install with: brew install llvm)
    endif
    CC := $(HOMEBREW_CLANG)
  endif
endif

# ── zstd/zlib system libraries ───────────────────────────────────
PQ_INCFLAGS =
PQ_LDFLAGS  =
ifeq ($(UNAME_S),Darwin)
  ZSTD_PREFIX := $(shell brew --prefix zstd 2>/dev/null)
  ZLIB_PREFIX := $(shell brew --prefix zlib 2>/dev/null)
  ifneq ($(ZSTD_PREFIX),)
    PQ_INCFLAGS += -I$(ZSTD_PREFIX)/include
    PQ_LDFLAGS  += -L$(ZSTD_PREFIX)/lib
  endif
  ifneq ($(ZLIB_PREFIX),)
    PQ_INCFLAGS += -I$(ZLIB_PREFIX)/include
    PQ_LDFLAGS  += -L$(ZLIB_PREFIX)/lib
  endif
endif
PQ_LDFLAGS += -lzstd -lz

# ── Compiler flags ───────────────────────────────────────────────
CFLAGS         ?= -Wall -Wextra -Wswitch-enum -Wpedantic -std=c11 -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE -O1 -g -fsanitize=address -fno-omit-frame-pointer -MMD -MP
CFLAGS         += $(PQ_INCFLAGS)
RELEASE_CFLAGS  = -Wall -Wextra -Wswitch-enum -Wpedantic -std=c11 \
  -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE \
  -O3 -flto -DNDEBUG -g -MMD -MP \
  -march=native -ffast-math -funroll-loops $(PQ_INCFLAGS)

# ── Sources and objects ──────────────────────────────────────────
BUILDDIR         = ../build
SRCS             = main.c database.c table.c query.c row.c parser.c pgwire.c index.c column.c plan.c catalog.c datetime.c parquet.c pq_reader.c hnsw.c diskio.c logical.c explain_ast.c
LIB_SRCS         = database.c table.c query.c row.c parser.c index.c column.c plan.c catalog.c datetime.c parquet.c pq_reader.c hnsw.c diskio.c logical.c explain_ast.c
OBJS             = $(patsubst %.c,$(BUILDDIR)/%.o,$(SRCS))
RELEASE_OBJS     = $(patsubst %.c,$(BUILDDIR)/rel_%.o,$(SRCS))
RELEASE_LIB_OBJS = $(patsubst %.c,$(BUILDDIR)/rel_%.o,$(LIB_SRCS))

# ── Targets ──────────────────────────────────────────────────────
TARGET         = $(BUILDDIR)/mskql_debug
RELEASE_TARGET = $(BUILDDIR)/mskql
BENCH          = $(BUILDDIR)/mskql_bench
CLI_DBG        = $(BUILDDIR)/mskqlcli_debug
CLI_REL        = $(BUILDDIR)/mskqlcli

all: $(TARGET) $(CLI_DBG)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) -lm $(PQ_LDFLAGS)

release: $(RELEASE_OBJS)
	$(CC) $(RELEASE_CFLAGS) -o $(RELEASE_TARGET) $(RELEASE_OBJS) -lm $(PQ_LDFLAGS)

bench: $(RELEASE_LIB_OBJS) $(BUILDDIR)/rel_bench.o
	$(CC) $(RELEASE_CFLAGS) -o $(BENCH) $(RELEASE_LIB_OBJS) $(BUILDDIR)/rel_bench.o -lm $(PQ_LDFLAGS)

$(CLI_DBG): mskqlcli.c | $(BUILDDIR)
	$(CC) $(CFLAGS) -o $@ $<

$(CLI_REL): mskqlcli.c | $(BUILDDIR)
	$(CC) $(RELEASE_CFLAGS) -o $@ $<

mskqlcli: $(CLI_REL)

mskqlcli-debug: $(CLI_DBG)

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
               parser.c index.c column.c plan.c catalog.c datetime.c \
               hnsw.c logical.c explain_ast.c
WASM_TARGET  = $(BUILDDIR)/mskql.wasm

wasm: wasm-stubs $(WASM_TARGET)

wasm-stubs: | $(BUILDDIR)
	@mkdir -p $(WASM_INCDIR) $(WASM_INCDIR)/sys
	@for h in stdio.h stdlib.h string.h strings.h stddef.h stdint.h stdbool.h stdarg.h ctype.h math.h time.h signal.h float.h unistd.h; do \
		echo '/* stub */' > $(WASM_INCDIR)/$$h; \
	done
	@echo '/* stub */' > $(WASM_INCDIR)/sys/stat.h

$(WASM_TARGET): $(WASM_SRCS) | $(BUILDDIR)
	$(WASM_CC) $(WASM_CFLAGS) $(WASM_LDFLAGS) -o $@ $(WASM_SRCS)

-include $(OBJS:.o=.d)
-include $(RELEASE_OBJS:.o=.d)

.PHONY: all clean release bench bench-throughput wasm mskqlcli mskqlcli-debug
