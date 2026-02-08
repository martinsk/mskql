CC       = cc
CFLAGS   = -Wall -Wextra -Wswitch-enum -Wpedantic -std=c11 -g
BUILDDIR = ../build
SRCS     = main.c database.c table.c query.c row.c parser.c pgwire.c index.c column.c
OBJS     = $(patsubst %.c,$(BUILDDIR)/%.o,$(SRCS))
TARGET   = $(BUILDDIR)/mskql

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

$(BUILDDIR)/%.o: %.c | $(BUILDDIR)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

clean:
	rm -rf $(BUILDDIR)

.PHONY: all clean
