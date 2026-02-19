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
CFLAGS  ?= -Wall -Wextra -Wswitch-enum -Wpedantic -std=c11 -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE -O1 -g -fsanitize=address -fno-omit-frame-pointer -MMD -MP
BUILDDIR = ../build
SRCS     = main.c database.c table.c query.c row.c parser.c pgwire.c index.c column.c plan.c catalog.c datetime.c parquet.c
OBJS     = $(patsubst %.c,$(BUILDDIR)/%.o,$(SRCS))
LIB_SRCS = database.c table.c query.c row.c parser.c index.c column.c plan.c catalog.c datetime.c parquet.c
LIB_OBJS = $(patsubst %.c,$(BUILDDIR)/%.o,$(LIB_SRCS))

# ── Carquet (auto-cloned Parquet library) ─────────────────────────
CARQUET_DIR    = ../third_party/carquet
CARQUET_BUILD  = $(BUILDDIR)/carquet
CARQUET_LIB    = $(CARQUET_BUILD)/libcarquet.a
CARQUET_INC    = $(CARQUET_DIR)/include
CARQUET_REPO   = https://github.com/Vitruves/carquet.git
CARQUET_COMMIT = 310d5ce58f9e59e9439a2dedabd22eead8d07271
CFLAGS        += -I$(CARQUET_INC)

# zstd/zlib library paths (Homebrew on macOS)
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  ZSTD_PREFIX := $(shell brew --prefix zstd 2>/dev/null)
  ZLIB_PREFIX := $(shell brew --prefix zlib 2>/dev/null)
  ifneq ($(ZSTD_PREFIX),)
    CARQUET_LDFLAGS += -L$(ZSTD_PREFIX)/lib
  endif
  ifneq ($(ZLIB_PREFIX),)
    CARQUET_LDFLAGS += -L$(ZLIB_PREFIX)/lib
  endif
endif
CARQUET_LDFLAGS += -lz -lzstd
TARGET   = $(BUILDDIR)/mskql_debug
BENCH    = $(BUILDDIR)/mskql_bench

# ── Release flags (for benchmarks / production) ──────────────────
RELEASE_CFLAGS = -Wall -Wextra -Wswitch-enum -Wpedantic -std=c11 -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE -O3 -flto -DNDEBUG -g -MMD -MP -I$(CARQUET_INC)
RELEASE_OBJS     = $(patsubst %.c,$(BUILDDIR)/rel_%.o,$(SRCS))
RELEASE_LIB_OBJS = $(patsubst %.c,$(BUILDDIR)/rel_%.o,$(LIB_SRCS))

all: $(CARQUET_LIB) $(TARGET)

$(CARQUET_DIR)/CMakeLists.txt:
	@echo "Cloning Carquet..."
	@git clone --quiet $(CARQUET_REPO) $(CARQUET_DIR)
	@cd $(CARQUET_DIR) && git checkout --quiet $(CARQUET_COMMIT)

$(CARQUET_LIB): $(CARQUET_DIR)/CMakeLists.txt | $(BUILDDIR)
	@cmake -S $(CARQUET_DIR) -B $(CARQUET_BUILD) -DCMAKE_BUILD_TYPE=Release \
		-DCARQUET_BUILD_TESTS=OFF -DCARQUET_BUILD_EXAMPLES=OFF \
		-DCARQUET_BUILD_BENCHMARKS=OFF -DCARQUET_BUILD_SHARED=OFF \
		>/dev/null 2>&1
	@$(MAKE) -C $(CARQUET_BUILD) --no-print-directory -j$$(nproc 2>/dev/null || sysctl -n hw.ncpu) >/dev/null 2>&1

OMP_LDFLAGS =
ifeq ($(UNAME_S),Darwin)
  OMP_LDFLAGS = -L/opt/homebrew/opt/llvm/lib -lomp
endif

$(TARGET): $(OBJS) $(CARQUET_LIB)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(CARQUET_LIB) -lm $(CARQUET_LDFLAGS) $(OMP_LDFLAGS)

RELEASE_TARGET = $(BUILDDIR)/mskql

release: $(CARQUET_LIB) $(RELEASE_OBJS)
	$(CC) $(RELEASE_CFLAGS) -o $(RELEASE_TARGET) $(RELEASE_OBJS) $(CARQUET_LIB) -lm $(CARQUET_LDFLAGS)

bench: $(CARQUET_LIB) $(RELEASE_LIB_OBJS) $(BUILDDIR)/rel_bench.o
	$(CC) $(RELEASE_CFLAGS) -o $(BENCH) $(RELEASE_LIB_OBJS) $(BUILDDIR)/rel_bench.o $(CARQUET_LIB) -lm $(CARQUET_LDFLAGS)

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

carquet-clean:
	rm -rf $(CARQUET_BUILD)

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

-include $(OBJS:.o=.d)
-include $(RELEASE_OBJS:.o=.d)

.PHONY: all clean carquet-clean release bench bench-throughput wasm
