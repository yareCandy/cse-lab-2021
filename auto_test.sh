#!/bin/bash
make clean &> /dev/null 2>&1
make &> /dev/null 2>&1

./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1

score=0

mkdir chfs1 >/dev/null 2>&1
mkdir chfs2 >/dev/null 2>&1

./start.sh &> /dev/null 2>&1

./test-lab2-part2-b.sh chfs2 

./stop.sh &> /dev/null 2>&1

