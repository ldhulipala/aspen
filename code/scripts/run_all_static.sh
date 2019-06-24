#!/bin/bash

declare -a graphs=("soc-LiveJournal1_sym.adj" "com-orkut.ungraph.adj" "twitter_sym.adj")
declare -a c_graphs=("clueweb_sym.bytepda" "hyperlink2014_sym.bytepda" "hyperlink2012_sym.bytepda")

# create the data dir
mkdir -p data/parallel_times/

# make sure the binary is built
make run_static_algorithm

for graph in "${c_graphs[@]}"; do
  numactl -i all ./run_static_algorithm -all -src 100000 -nsrc 120 -c -nparts 16 -s -m -f inputs/${graph} > data/parallel_times/${graph}-par.dat

  # run sequentially
  export NUM_THREADS=1
  numactl -i all ./run_static_algorithm -all -src 100000 -nsrc 120 -c -nparts 16 -s -m -f inputs/${graph} > data/parallel_times/${graph}-seq.dat
  unset NUM_THREADS
done

for graph in "${graphs[@]}"; do
    numactl -i all ./run_static_algorithm -all -src 10012 -nsrc 576 -s -m -f inputs/${graph} > data/parallel_times/${graph}-par.dat

    # run sequentially
    export NUM_THREADS=1
    ./run_static_algorithm -all -src 10012 -nsrc 576 -s -m -f inputs/${graph} > data/parallel_times/${graph}-seq.dat
    unset NUM_THREADS
done
