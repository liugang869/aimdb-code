CC = g++
CFLAGS = -Wall -g -O2 -std=c++11 

INCLUDE = -I.
CFLAGS += $(INCLUDE)

CPPS = $(shell find ./ -name "*.cc")
OBJS = $(CPPS:.cc=.o)
DEPS = $(CPPS:.cc=.d)

all: runaimdb

runaimdb : $(OBJS)
	$(CC) -o $@ $^ $(CFLAGS)

-include $(OBJS:%.o=%.d)

%.d: %.cc
	$(CC) -MM -MT $*.o -MF $@ $< $(CFLAGS)

%.o: %.cc
	$(CC) -c $(CFLAGS) -o $@ $<

clean:
	rm -f runaimdb $(OBJS) $(DEPS)
