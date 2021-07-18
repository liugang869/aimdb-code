CC = g++
CFLAGS = -Wall -g -O2 -std=c++11

INCLUDE = -I../../system/
CFLAGS += $(INCLUDE)

CPPS = $(shell find ./ -name "*.cc")
OBJS = $(shell find ../system/ -name "*.o")

TARGET = runtpcc

all: ${TARGET}

runtpcc :  
	$(CC) -o $@ runtpcc.cc $(CFLAGS) ../../system/catalog.o ../../system/mymemory.o ../../system/global.o \
		../../system/hashindex.o ../../system/rowtable.o ../../system/hashtable.o ../../system/pbtreeindex.o ../../system/pbtree.o

clean:
	rm runtpcc
