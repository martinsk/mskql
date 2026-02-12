CC       = cc
CFLAGS  ?= -Wall -Wextra -Wswitch-enum -Wpedantic -std=c11 -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE -O3 -flto -DNDEBUG -g
BUILDDIR = ../build
SRCS     = main.c database.c table.c query.c row.c parser.c pgwire.c index.c column.c plan.c
OBJS     = $(patsubst %.c,$(BUILDDIR)/%.o,$(SRCS))
LIB_SRCS = database.c table.c query.c row.c parser.c index.c column.c plan.c
LIB_OBJS = $(patsubst %.c,$(BUILDDIR)/%.o,$(LIB_SRCS))
TARGET   = $(BUILDDIR)/mskql
BENCH    = $(BUILDDIR)/mskql_bench

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ -lm

bench: $(LIB_OBJS) $(BUILDDIR)/bench.o
	$(CC) $(CFLAGS) -o $(BENCH) $^ -lm

$(BUILDDIR)/bench.o: ../bench/bench.c | $(BUILDDIR)
	$(CC) $(CFLAGS) -c -o $@ $<

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
               parser.c index.c column.c plan.c
WASM_TARGET  = $(BUILDDIR)/mskql.wasm

wasm: wasm-stubs $(WASM_TARGET)

wasm-stubs: | $(BUILDDIR)
	@mkdir -p $(WASM_INCDIR)
	@for h in stdio.h stdlib.h string.h strings.h stddef.h stdint.h stdbool.h ctype.h math.h time.h signal.h; do \
		echo '/* stub */' > $(WASM_INCDIR)/$$h; \
	done

$(WASM_TARGET): $(WASM_SRCS) | $(BUILDDIR)
	$(WASM_CC) $(WASM_CFLAGS) $(WASM_LDFLAGS) -o $@ $(WASM_SRCS)

.PHONY: all clean bench wasm
