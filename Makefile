all: pmerge.cpp record.h
	g++ -ggdb -std=c++11 -lpthread -o proj2 pmerge.cpp

clean:
	rm -rf *.o
