#!/bin/bash

# must run from AspenAE/code directory!

declare -a algos=("BFS" "BC" "MIS")
declare -a graphs=("twitter_sym.adj")
declare -a sizes=(1 2 3 4 5 6 7 8 9 10 11 12)

# create the data dir
mkdir -p data/chunk_sizes/

# make sure the binary is built
make run_static_algorithm

for graph in "${graphs[@]}"; do
  for algo in "${algos[@]}"; do
    for sizes in "${sizes[@]}"; do

      numactl -i all ./run_static_algorithm -rounds 12 -t ${algo} -src 10012 -headfreq ${sizes} -s -m -f inputs/${graph} > data/chunk_sizes/${graph}-${algo}-${sizes}.dat

    done
  done
done
