CC       = cc
CFLAGS   = -Wall -Wextra -Wpedantic -std=c11 -D_POSIX_C_SOURCE=200809L -g
BUILDDIR = ../../../build
TARGET   = $(BUILDDIR)/test_extended

all: $(TARGET)

$(TARGET): test_extended.c | $(BUILDDIR)
	$(CC) $(CFLAGS) -o $@ $<

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

clean:
	rm -f $(TARGET)

run: $(TARGET)
	$(TARGET)

.PHONY: all clean run
