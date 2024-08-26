#!/bin/bash

# compile
make -j 4

if [ $? -ne 0 ]; then
    echo "Compilation failed"
    exit 1
fi

if [ ! -f ./mr_coordinator ]; then
    echo "mr_coordinator not found"
    exit 1
fi

# check for output files
if ls mr-*.txt 1> /dev/null 2>&1; then
  ./clear_output.sh
fi

num_workers=3

./mr_coordinator "$num_workers" &

server_pid=$!

sleep 1

for ((i=0; i<=num_workers-1; i++))
do
  ./mr_worker "$i" &
done
