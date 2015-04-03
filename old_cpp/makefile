CC=g++-4.9
CFLAGS=-fopenmp -std=c++11
INCLUDES=-I./lib

all: test

#test: test.cpp utils/iofuncs.cpp
#	$(CC) $(CFLAGS) -I/Users/pengxiang/GitHub/distvec/lib test.cpp utils/iofuncs.cpp -o test

#test: test.cpp distvec.cpp iofuncs.cpp
#	$(CC) $(CFLAGS) -I/Users/pengxiang/GitHub/distvec/lib test.cpp distvec.cpp iofuncs.cpp -o test

test: test.o distvec.o iofuncs.o
	$(CC) $(CFLAGS) $(INCLUDES) test.o distvec.o iofuncs.o -o test

test.o: test.cpp types.h
	$(CC) $(CFLAGS) $(INCLUDES) -c test.cpp

distvec.o: distvec.cpp distvec.h types.h
	$(CC) $(CFLAGS) $(INCLUDES) -c distvec.cpp

iofuncs.o: iofuncs.cpp iofuncs.h types.h
	$(CC) $(CFLAGS) $(INCLUDES) -c iofuncs.cpp

clean:
	rm *.o test

