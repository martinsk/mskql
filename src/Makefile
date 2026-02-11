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
	$(CC) $(CFLAGS) -o $@ $^

bench: $(LIB_OBJS) $(BUILDDIR)/bench.o
	$(CC) $(CFLAGS) -o $(BENCH) $^

$(BUILDDIR)/bench.o: ../bench/bench.c | $(BUILDDIR)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BUILDDIR)/%.o: %.c | $(BUILDDIR)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

clean:
	rm -rf $(BUILDDIR)

.PHONY: all clean bench
