# Compiles the two executables.

all: jobCommander jobExecutorServer

jobCommander: jobCommander.cpp
	g++ -o jobCommander jobCommander.cpp

jobExecutorServer: jobExecutorServer.cpp
	g++ -o jobExecutorServer jobExecutorServer.cpp -std=c++11 -pthread