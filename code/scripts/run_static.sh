#!/bin/bash

# must run from AspenAE/code directory!

declare -a algos=("BFS" "BC" "MIS")

declare -a graphs=("soc-LiveJournal1_sym.adj" "com-orkut.adj" "twitter_sym.adj")

# create the data dir
mkdir -p data/par_scalability/

# make sure the binary is built
make run_static_algorithm

for graph in "${graphs[@]}"; do
  for algo in "${algos[@]}"; do

    numactl -i all ./run_static_algorithm -t ${algo} -src 10012 -s -m -f inputs/${graph} > data/par_scalability/${graph}-${algo}-par.dat

    # run sequentially
    export NUM_THREADS=1
    ./run_static_algorithm -t ${algo} -src 10012 -s -m -f inputs/${graph} > data/par_scalability/${graph}-${algo}-seq.dat
    unset NUM_THREADS

  done
done
