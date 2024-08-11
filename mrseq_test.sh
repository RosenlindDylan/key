#!/bin/bash

# compile
g++ -o mrseq_ts ./mrseq.cpp

if [ $? -ne 0 ]; then
    echo "Compilation failed"
    exit 1
fi

# run the file on one of the txt files
./mrseq_ts pg-being_ernest.txt pg-dorian_gray.txt pg-frankenstein.txt pg-grimm.txt pg-huckleberry_finn.txt pg-metamorphosis.txt pg-sherlock_holmes.txt pg-tom_sawyer.txt

cat output.txt

# clean up
rm mrseq_ts