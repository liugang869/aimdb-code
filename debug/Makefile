CC = g++
CFLAGS = -Wall -g -O2 -std=c++11

INCLUDE = -I../system/
CFLAGS += $(INCLUDE)

CPPS = $(shell find ./ -name "*.cc")
OBJS = $(shell find ../system/ -name "*.o")

TARGET = debug_catalog debug_datatype debug_hashtable debug_hashindex debug_mymemory debug_schema debug_errorlog debug_rowtable debug_pbtree debug_pbtreeindex debug_executor

all: ${TARGET}

debug_catalog :  
	$(CC) -o $@ debug_catalog.cc $(CFLAGS) ../system/catalog.o ../system/mymemory.o \
		../system/hashindex.o ../system/rowtable.o ../system/hashtable.o ../system/pbtreeindex.o ../system/pbtree.o

debug_datatype : 
	$(CC) -o $@ debug_datatype.cc $(CFLAGS)

debug_hashindex : 
	$(CC) -o $@ debug_hashindex.cc $(CFLAGS) ../system/hashindex.o ../system/mymemory.o \
		../system/hashtable.o

debug_mymemory : 
	$(CC) -o $@ debug_mymemory.cc $(CFLAGS) ../system/mymemory.o

debug_schema : 
	$(CC) -o $@ debug_schema.cc $(CFLAGS)

debug_errorlog : 
	$(CC) -o $@ debug_errorlog.cc $(CFLAGS) ../system/errorlog.o -lpthread

debug_hashtable : 
	$(CC) -o $@ debug_hashtable.cc $(CFLAGS) ../system/mymemory.o ../system/hashtable.o

debug_rowtable : 
	$(CC) -o $@ debug_rowtable.cc $(CFLAGS) ../system/rowtable.o ../system/mymemory.o

debug_executor :
	$(CC) -o $@ debug_executor.cc $(CFLAGS)

debug_pbtree :
	$(CC) -o $@ debug_pbtree.cc $(CFLAGS) ../system/mymemory.o ../system/pbtree.o

debug_pbtreeindex :
	$(CC) -o $@ debug_pbtreeindex.cc $(CFLAGS) ../system/mymemory.o ../system/pbtree.o ../system/pbtreeindex.o

clean:
	rm -f ${TARGET}
