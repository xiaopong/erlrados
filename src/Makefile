##
## Copyright (C) 2012, xp@renzhi.ca
## All rights reserved.
##

CC=g++
#CFLAGS = -g -DGC_MALLOC_CHECK=1 -fPIC -fpermissive -D__DEBUG
CFLAGS = -g -DGC_MALLOC_CHECK=1 -fPIC -fpermissive
LIBDIR=-L.
LIBS=-lrados

OUT=rados_nif.so
OUTDEST=..

SRC=rados_nif.cpp rados_cluster.cpp rados_pool.cpp rados_io.cpp \
	rados_aio.cpp rados_xattr.cpp rados_snap.cpp \
	fsutil.cpp mutex.cpp tmutil.cpp log.cpp

OBJ=$(SRC:.cpp=.o)

# include directories
INCLUDES = -I. -I../include

$(OUT): $(OBJ)
	@echo Archiving $(OUT)
	@$(CC) -shared -o $(OUT) $(OBJ) $(LIBDIR) $(LIBS)

.cpp.o:
	@echo Compiling $<
	@$(CC) $(INCLUDES) $(CFLAGS) -c $< -o $@

install: $(OUT)
	@install -m 755 $(OUT) $(OUTDEST)

clean:
	rm -f $(OBJ) $(OUT) Makefile.bak 
