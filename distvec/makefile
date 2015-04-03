CC=g++
CFLAGS=-fopenmp -std=c++0x
INCLUDES=-I./lib

all: trainplf compsim

trainplf: trainplf.o distvec.o utils.o
	$(CC) $(CFLAGS) $(INCLUDES) trainplf.o distvec.o utils.o -o trainplf

trainplf.o: trainplf.cpp types.h
	$(CC) $(CFLAGS) $(INCLUDES) -c trainplf.cpp

distvec.o: distvec.cpp distvec.h types.h redsvd.h
	$(CC) $(CFLAGS) $(INCLUDES) -c distvec.cpp

utils.o: utils.cpp utils.h types.h
	$(CC) $(CFLAGS) $(INCLUDES) -c utils.cpp

compsim: compsim.o phsim.o utils.o
	$(CC) $(CFLAGS) $(INCLUDES) compsim.o phsim.o utils.o -o compsim

compsim.o: compsim.cpp types.h
	$(CC) $(CFLAGS) $(INCLUDES) -c compsim.cpp

phsim.o: phsim.cpp phsim.h types.h
	$(CC) $(CFLAGS) $(INCLUDES) -c phsim.cpp

clean:
	rm *.o trainplf compsim

