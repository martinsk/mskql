CC      = cc
CFLAGS  = -Wall -Wextra -std=c11 -g
SRCS    = main.c database.c table.c query.c row.c parser.c pgwire.c index.c column.c
OBJS    = $(SRCS:.c=.o)
TARGET  = mskql

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean
