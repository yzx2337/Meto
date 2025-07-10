.PHONY: all clean

CXX := g++ -O3 -std=c++17
CFLAGS := -I./ -lrt

all: test

test: src/test.cpp YMIX
	$(CXX) $(CFLAGS) -o bin/single_threaded_YMIX src/test.cpp src/YMIX.o include/dlock.o  -lpmemobj -lpmem -lpqos   
	$(CXX) $(CFLAGS) -o bin/multi_threaded_YMIX src/test.cpp src/YMIX.o include/dlock.o  -lpmemobj -lpmem -lpthread -DMULTITHREAD -lpqos   
	$(CXX) $(CFLAGS) -o bin/single_threaded_YMIXCoW src/test.cpp src/YMIX_CoW.o include/dlock.o -lpmemobj -lpmem -lpqos   
	$(CXX) $(CFLAGS) -o bin/multi_threaded_YMIXCoW src/test.cpp src/YMIX_CoW.o include/dlock.o -lpmemobj -lpmem -lpthread -DMULTITHREAD -lpqos   

YMIX: src/YMIX.h src/YMIX.cpp dlock
	$(CXX) $(CFLAGS) -c -o src/YMIX.o src/YMIX.cpp  -DINPLACE -lpmemobj -lpmem 
	$(CXX) $(CFLAGS) -c -o src/YMIX_CoW.o src/YMIX.cpp  -lpmemobj -lpmem 

dlock: include/dlock.c
	$(CXX) $(CFLAGS) -c -o include/dlock.o include/dlock.c -lpqos   
	
clean:
	rm -rf src/*.o bin/* 
