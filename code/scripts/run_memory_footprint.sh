#!/bin/bash

#declare -a graphs=("soc-LiveJournal1_sym.adj" "com-orkut.ungraph.adj" "twitter_sym.adj")
#declare -a c_graphs=("clueweb_sym.bytepda" "hyperlink2014_sym.bytepda" "hyperlink2012_sym.bytepda")
declare -a c_graphs=("hyperlink2012_sym.bytepda")

# create the data dir
mkdir -p data/memory_footprint/

# make sure the binary is built
make memory_footprint

for graph in "${graphs[@]}"; do
    numactl -i all ./memory_footprint -s -m -f inputs/${graph} > data/memory_footprint/${graph}.dat
done

for graph in "${c_graphs[@]}"; do
  numactl -i all ./memory_footprint -c -nparts 16 -s -m -f inputs/${graph} > data/memory_footprint/${graph}.dat
done
