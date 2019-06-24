# Aspen

This repository provides  experiments, scripts, and instructions for reproducing
the experiments in our paper, *Low-Latency Graph Streaming Using Compressed
Purely-Functional Trees*. Our paper introduces Aspen, a graph-streaming system
based on compressed purely-functional trees.  Aspen is designed for maintaining
a dynamic graph subject to updates by a single writer, while supporting multiple
concurrent readers. Due to the fact that the graph is purely-functional, all
operations in Aspen are strictly serializable.

In the Getting Started Guide, we include functionality to reproduce the main
results presented in our paper.

In the Step-By-Step Instructions, we also include the codes and instructions
used for running experiments on very large graphs (hundreds of billions of
edges). Due to the size of these graphs, and the memory footprint requirement on
the machine, and the time to download, convert, and process these graphs, we
expect that most users will not perform these steps, but we include them for
completeness, and to ensure that our results for very large graphs are
reproducible. We have made all graphs used in our paper publicly-available to
ensure that our results are reproducible and can be built upon.

# Getting Started Guide
This Getting Started Guide gives an overview of
1. Using Aspen as a Graph-Streaming System

2. Setting up Aspen
  *  Hardware and software dependencies
  *  Input formats
  *  Obtaining datasets

3. Experiment Workflow:
  * Memory-footprint improvement due to compression and blocking
  * Performance and speedup of parallel graph algorithms
  * Microbenchmarks measuring the throughput of batch operations


## Using Aspen as a Graph-Streaming System
We give a brief overview of the user-level API provided by Aspen for running
graph algorithms, and performing updates.

An initial static graph can be generated using the `initialize_graph` function,
which takes as input several optional arguments and returns a `versioned_graph`.

```
/* Creates a versioned graph based on an initial static graph snapshot */
initialize_graph(string fname="", bool mmap=false, bool is_symmetric=true,
                 bool compressed=false, size_t n_parts=1) -> versioned_graph
```

The `versioned_graph` type has the following interface. It is a _single-writer,
multi-reader interface_, i.e., it can be read by multiple concurrent readers,
which can _acquire_ a version of the graph to perform queries or graph analyses,
and a single writer, which can perform updates. The type has the following
user-facing interface:

```
/* Used by readers to atomically acquire a snapshot */
acquire_version() -> snapshot_graph

/* Used by a single writer to insert a batch of edges */
insert_edges_batch(size_t m, tuple<uintV, uintV>* edges, bool sorted=false,
                   bool remove_dups=false, bool run_seq=false) -> void

/* Used by a single writer to delete a batch of edges */
delete_edges_batch(size_t m, tuple<uintV, uintV>* edges, bool sorted=false,
                   bool remove_dups=false, bool run_seq=false) -> void
```

The graph snapshot type `snapshot_graph` has the following interface:
```
/* Returns the number of vertices in the graph */
num_vertices() -> size_t

/* Returns the number of edges in the graph */
num_edges() -> size_t

/* Creates a flat snapshot of the vertices */
create_flat_snapshot() -> flat_snapshot

/* Applies f to each (v, u) edge for v in vs and returns a *
 * vertex_subset containing u such that f(v, u) = true           */
template <class F>
edge_map(vertex_subset& vs, F f, long threshold=-1) -> vertex_subset
```

The interface for `snapshot_graph` is similar to the Ligra interface, enabling
users to implement and run existing Ligra algorithms with only minor cosmetic
modifications. We provide the following examples of using the interface in the
`algorithms` directory:
```
BC.h      /* single-source betweenness centrality */
BFS.h     /* breadth-first search */
MIS.h     /* maximal independent set */
k-Hop.h   /* local algorithm to find the k-hop neighborhood of a vertex */
Nibble.h  /* local clustering algorithm based on Spielman-Teng */
```

## Setting up Aspen

### Hardware Dependencies

Any modern x86-based multicore machine can be used. Aspen uses 128-bit CMPXCHG
(requires -mcx16 compiler flag) but does not need hardware transactional memory
(TSX). Most (small graph) examples in our artifact can be run using fewer than
100GB of memory, but our tests on larger graph (ClueWeb and larger) require
200GB--1TB of memory. All examples below can run in 100GB of memory by using a
small graph input.

### Software Dependencies
Aspen requires g++ 5.4.0 or later versions supporting the Cilk Plus extensions.
The scripts that we provide in the repository use `numactl -i all` for better
performance. However, all tests can also run directly without `numactl`.

### Input Formats

Aspen currently supports reading two formats: the adjacency graph format used by the [Problem Based Benchmark
suite](http://www.cs.cmu.edu/~pbbs/benchmarks/graphIO.html)
and [Ligra](https://github.com/jshun/ligra), and a compressed graph format
developed as part of the [Graph Based Benchmark
Suite](https://github.com/ldhulipala/gbbs).

The adjacency graph format starts with a sequence of offsets one for each
vertex, followed by a sequence of directed edges ordered by their source vertex.
The offset for a vertex i refers to the location of the start of a contiguous
block of out edges for vertex i in the sequence of edges. The block continues
until the offset of the next vertex, or the end if i is the last vertex. All
vertices and offsets are 0 based and represented in decimal. The specific format
is as follows:

```
AdjacencyGraph
<n>
<m>
<o0>
<o1>
...
<o(n-1)>
<e0>
<e1>
...
<e(m-1)>
```
This file is represented as plain text.

The compressed format is the `bytePDA` format, which is similar to the
parallelByte format of Ligra+, extended with additional functionality.

Note that for the artifact, we limit Aspen to processing symmetric, unweighted
graph datasets. The version that we will ultimately release on Github will support
both undirected and directed (weighted) graphs.

### Obtaining the Datasets
The small graphs used in our experiments can be obtained from the [Stanford SNAP](http://snap.stanford.edu/data/index.html)
repository. We recommend using the soc-LiveJournal graph, and have provided a
python script to download this graph, symmetrize it, and store it in the
the format(s) used by Aspen. This can be done using the SNAPToAdj software in
the [Ligra](https://github.com/jshun/ligra) repository (see
`ligra/utils/SNAPToAdj.C`).

Alternately, we have made all graphs used in our experiments publicly-available
on a self-hosted machine. Downloading the graphs from this location is likely
the fastest way to get up and running. The files are located at
`https://metis.feralhosting.com/laxman/graphs`, and can be downloaded
using `wget`, as follows:
```
# listing of  https://metis.feralhosting.com/laxman/graphs/
clueweb_sym.bytepda                                21-Feb-2019 04:45     99G
com-orkut.adj                                      23-Feb-2019 05:37      2G
hyperlink2012_sym.bytepda                          22-Feb-2019 02:00    351G
hyperlink2014_sym.bytepda                          21-Feb-2019 07:58    184G
soc-LiveJournal1_sym.adj                           23-Feb-2019 05:38    640M
twitter_SJ.adj                                     23-Feb-2019 05:58     20G

# Download the soc-LiveJournal graph to the inputs/ directory
$ wget https://metis.feralhosting.com/laxman/graphs/soc-LiveJournal1_sym.adj inputs/
```


## Experiment Workflow

This section assumes that you have downloaded some of the input graphs listed
above to the `inputs/` folder.

All of the applications below share a common set of flags, which we list below:
```
-f filename     : provides a location to an input graph file
-s              : indicates that the input graph is symmetric
-c              : indicates that the input graph is compressed
-m              : indicates that the input graph should be mmap'd
```

#### Memory Usage in Aspen

The memory usage of our codes can be measured using a tool called
`memory_footprint`, which loads a graph using the C-tree data structure and
outputs the number of bytes used by the C-tree representation. In our
experiments, we set the probability of a node being selected as a _head_ to
p = 1/256, so the expected number of nodes in the edges tree is p*m.

Using
```
$ make memory_footprint
g++ -O3 -DEDGELONG -mcx16 -march=native -fcilkplus -std=c++14 tools/memory_footprint.cpp -o memory_footprint
```
will give the executable file `memory_footprint`.

It can be used as follows to reproduce the results in Table 2 in [1]:
```
# ./memory_footprint [-f graph_file]
$ ./memory_footprint -s -f inputs/soc-LiveJournal1_sym.adj
Running Aspen using 144 threads.
n = 4847571 m = 85702474

== Aspen Difference Encoded Stats (DE) ==
vertex tree: used nodes = 4847571 size/node = 64 memory used = 0.288938 GB
edge bytes: used nodes = 337050 size/node = 48 memory used = 0.0150673 GB
compressed-lists memory used = 0.26274 GB
Total: 0.566745 GB

== Aspen No Difference Encoding Stats (No DE) ==
vertex tree: used nodes = 4847571 size/node = 64 memory used = 0.288938 GB
edge bytes: used nodes = 337050 size/node = 48 memory used = 0.0150673 GB
compressed-lists (no difference-encoding) memory used = 0.480281 GB
Total: 0.784286 GB

== Aspen No C-Trees Stats (Uncompressed) ==
vertex tree: used nodes = 4847571 size/node = 48 memory used = 0.216703 GB
edge tree: used nodes = 85702474 size/node = 32 memory used = 2.55413 GB
Total: 2.77084 GB
```
Please note that we standardized
the parameters used for the C-Tree experiments with and without difference
encoding, and also made a small memory-efficiency improvement after our
submission, which makes the size of the Aspen C-tree experiments slightly
smaller than the numbers reported in the paper. The new numbers (which are
strictly smaller, giving better savings) will be updated in the camera-ready
copy of the paper.

#### Static Algorithm Performance in Aspen

The algorithms described above can be run over a static graph to measure the
performance and speedup using the `run_static_algorithm` tool. The tool takes as input
the name of the algorithm to run, the input graph filename, and several other
parameters configuring how the graph should be loaded.

Using
```
$ make run_static_algorithm
g++ -O3 -DEDGELONG -mcx16 -march=native -fcilkplus -std=c++14 tools/run_static_algorithm.cpp -o run_static_algorithm
```
will give the executable file `run_static_algorithm`.

It can be used as follows:
```
# ./run_static_algorithm [-t testname -r rounds -f graph_file]
#   where testname is one of: {BFS, BC, MIS, KHOP, NIBBLE}
$ numactl -i all ./run_static_algorithm -t BFS -src 10012 -s -f inputs/twitter_sym.adj
Running Aspen using 144 threads.
n = 41652231 m = 2405026092
Running BFS from source = 10012
RESULT  test=BFS        time=0.172245   iteration=0     p=144
Running BFS from source = 10012
RESULT  test=BFS        time=0.174843   iteration=1     p=144
Running BFS from source = 10012
RESULT  test=BFS        time=0.175072   iteration=2     p=144
Running BFS from source = 10012
RESULT  test=BFS        time=0.178104   iteration=3     p=144
PARALLEL RESULT (AVG)   test=BFS        time=0.175066   p=144

Running sequential time
Running BFS from source = 10012
SEQUENTIAL RESULT       test=BFS        time=12.424587  iteration=0     p=1
```

The command outputs both the parallel times and the sequential times of the algorithms.
The self-relative speedup of our code can be calculated by dividing the parallel
time by the sequential time. We recommend using `numactl -i all` on multi-socket
machines on all parallel tests. Note that the sequential times obtained by this
experiment may be larger than those reported in the paper, especially if
`numactl -i all` is used.

To reproduce the sequential times reported in our paper we recommend running the
`run_static_algorithm` utility by setting the `CILK_NWORKERS` environment variable to
1, as below. The downside is that loading and building the graph has to be done
sequentially, which can take a long time on very large graphs. The advantage is
that on a multi-socket machine the memory will be allocated on a single node,
and the timing will not be affected by NUMA latency.
```
$ CILK_NWORKERS=1 ./run_static_algorithm -t BFS -src 10012 -s -f inputs/soc-LiveJournal1_sym.adj
```

##### Local algorithms
Our local algorithms are run with parallelism over the queries. Since the
local queries (e.g., k-Hop) can use a significant amount of memory on the large
graphs (the 2-hops of these graphs can consist of a significant fraction of the
vertices), the number of parallel queries issued should be smaller on the
larger graphs. We have provided the exact parameters used in our experiments in
the following scripts: `scripts/run_khop.sh` and `scripts/run_nibble.sh`.  The
data for each test will be written to
`data/static_algorithms/khop/graph.dat` and
`data/static_algorithms/nibble/graph.dat` for the khop and nibble scripts,
respectively.


#### Dynamic Operations in Aspen (Batch Updates)
Using
```
make run_batch_updates
```
will give the executable file `run_batch_updates`.

It can be used as follows:
```
# ./run_batch_updates [-f graph_file]
$ numactl -i all ./run_batch_updates -s -f /ssd1/graphs/bench_experiments/soc-LiveJournal1_sym.adj
Running with 144 threads
n = 4847571 m = 85702474
Running bs: 10
Avg insert: 0.000213703
Avg delete: 0.000209332

Running bs: 100
Avg insert: 0.000360648
Avg delete: 0.000368357

...

Running bs: 1000000000
Avg insert: 2.6432
Avg delete: 1.81675

Running bs: 2000000000
Avg insert: 4.92558
Avg delete: 3.24599
```

We have provided a script to run the batch update algorithm on all of our inputs
in `scripts/run_batch_updates.sh`.
The following command will run the same experiments used to generate the results from Table 5 in [1]:
```
./scripts/run_batch_updates.sh
```
The data for each graph will be written to `data/batch_updates/graph.dat`.

#### Simultaneous Updates and Queries
Using
```
make run_simultaneous_updates_queries
```
will give the executable file `run_simultaneous_updates_queries`.

It can be used as follows:
```
# ./run_simultaneous_updates_queries [-f graph_file]
$ numactl -i all ./run_simultaneous_updates_queries -queryiters 200 -m -s -f inputs/soc-LiveJournal_sym.adj
Running with 144 threads
n = 4847571 m = 85702474
Shuffling updates
SeqUpdateParQuery:
Started updates
Started queries
Update throughput = 101011
Average_latency = 9.89992e-06
Average query time : 0.0535617
```
The command also takes the flags `-noupdate`, which only runs queries, and `-noquery`, which only runs updates.

We have provided a script to run the batch update algorithm on all of our inputs
in `scripts/run_simultaneous_updates_queries.sh`.

The following command will run the same experiments used to generate the results from Table 5 in [1]:
```
./scripts/run_simultaneous_updates_queries.sh
```
The data for each graph will be written to `data/simultaneous_updates_queries/graph.dat`.


# Step-by-Step Instructions
In this section of our artifact, we describe how to set up and run experiments on
the large Web graphs used in our paper.


#### Loading Compressed Graphs

This section assumes that some of the large compressed graphs listed in the
_Obtaining the Datasets_ section have been stored in the `inputs/` directory.
All of the commands given in the previous section can be used on compressed
graphs by supplying the `-c` flag, in addition to the filename. Since our inputs
in this artifact are symmetric, the `-s` flag is also expected.

The large graphs require an extra parameter, `-nparts`, which controls the
number of batches that the input is broken up into when loading. Since the
input to `insert_edges_batch` take an uncompressed array of edge pairs, we break
the edges in the input into a small number of batches and load each batch
sequentially. The number of parts used for each of the large compressed graphs
on our 1TB machine is listed below:
```
clueweb: 8
hyperlink2014: 10
hyperlink2012: 16
```
Note that if you are running on a machine with less memory, you may need to
increase the number of parts to ensure that the operations fit within memory.

We have provided examples of loading and running experiments on the compressed
graphs in the scripts mentioned above (`scripts/run_khop.sh`,
`scripts/run_nibble.sh`, `scripts/run_batch_updates.sh`, and
`scripts/run_simultaneous_updates_queries.sh`). Note that the experiments for
compressed graphs in these scripts are commented out by default, and should be
uncommented to run the experiments.

### Comparison With STINGER
We list the settings and details used for our experiments with STINGER below.

#### Downloading and Compiling
We downloaded STINGER from its [github repository](https://github.com/stingergraph/stinger).

We used the following variable settings in STINGER/CMakeLists.txt, which
produced the fastest times and lowest memory usage based on our experiments.
```
# Memory size configuration bits for stinger_core
set(STINGER_DEFAULT_VERTICES "(1L<<30)" CACHE STRING "Default number of vertices")
set(STINGER_DEFAULT_NUMETYPES "1" CACHE STRING "Default number of edge types")
set(STINGER_DEFAULT_NUMVTYPES "1" CACHE STRING "Default number of vertex types")
set(STINGER_DEFAULT_NEB_FACTOR "1" CACHE STRING "Default number of edge blocks per vertex")
set(STINGER_EDGEBLOCKSIZE "14" CACHE STRING "Number of edges per edge block")
set(STINGER_NAME_STR_MAX "255" CACHE STRING "Max string length in physmap")
```

#### Batch Updates
After building STINGER we ran the following command to launch a STINGER server
with enough memory:
```
numactl -i all env STINGER_MAX_MEMSIZE=100G ./bin/stinger_server
```
On a separate prompt, we ran a batch-update job as follows:
```
# ./stinger_rmat_edge_generator [-p port] [-a server_addr] [-n num_vertices] [-x batch_size] [-y num_batches] [-i]
./bin/stinger_rmat_edge_generator -n 536870912 -x 1000000
```
which inserts batches of 1M edges at a time, where edges are sampled from an RMAT generator, and the number of vertices is 2^29.
We obtained timings for smaller batch sizes by adjusting the `-x` parameter.

#### Inputs
We converted our three smallest input graphs to the `.gr` file format (see the
[DIMACS challenge](http://users.diag.uniroma1.it/challenge9/format.shtml#graph)
description), one of the formats that is supported by STINGER.

#### Static Algorithm Performance and Memory Usage
We obtained the memory usage for STINGER on our input graphs using the following C++ routine:
```
calculate_stinger_size(nv, nebs, netypes, nvtypes);
```
and printing out the reported size after the initial graph is constructed.

We implemented a sparse BFS algorithms based on the existing STINGER
single-source betweenness centrality algorithm. Our static graph algorithm
experiments work by first loading the input graph into memory, and then using
one of the algorithm binaries to register an algorithm, e.g.:
```
$ env STINGER_MAX_MEMSIZE=300G numactl -i all ./bin/stinger_server -i /ssd0/graphs/bench_experiments/twitter_logn.gr -t d
$ ./bin/stinger_betweenness -s 1 -x
```

Note that we compared the STINGER code to a sparse BFS implemented in Aspen to
ensure a fair comparison. We will make this code available by passing a flag to
the `run_static_algorithm` binary in the final version of this artifact.

### Comparison With LLAMA
We list the settings and details used for our experiments with LLAMA below.

#### Downloading and Compiling
We downloaded LLAMA from its [github
repository](https://github.com/goatdb/llama).

We built the in-memory version of LLAMA by running
```
$ make benchmark-memory
```
Note that we were unable to get the dynamic batch-updates to work in LLAMA, and
did therefore limit our discussion here to benchmarking parallel graph
algorithms and memory usage.

### Inputs
We generated XStream (`.xs1`) graph inputs for LLAMA. We will provide the
`AspenToXS1` utility which converts one of our inputs to the XS1 format (the
code is currently written as part of the GBBS library).


#### Static Algorithm Performance and Memory Usage
Since LLAMA does not support a method for reporting its internal memory usage
(and we could not figure out a simple way to obtain an exact number), we
calculated the memory usage using the `ps_mem.py` [script](https://github.com/pixelb/ps_mem).

We modified the `bc_random` routine to compute the single-source betweenness
centrality. The algorithm implementation is identical, but we removed a
loop that ran multiple independent betweenness computations in parallel. We also
added timers around the algorithm invocations to measure just the algorithm
running time. No other LLAMA code was modified.

We ran the algorithms as follows:
```
$ numactl -i all ./benchmark-memory -c 3 -R 10012 -r bfs_count /ssd1/graphs/soc-LJ.xs1
$ numactl -i all ./bin/benchmark-memory -c 3 -R 10012 -r bc_random /ssd1/graphs/soc-LJ.xs1
```

### Comparisons with GAP, Ligra and Galois

We did not make any modifications to these libraries, and followed the READMEs
on their respective sources (links below) to compile and run these codes. We
used `numactl -i all` when running all applications.

* [GAPBS](https://github.com/sbeamer/gapbs)
* [Ligra](https://github.com/jshun/ligra)
* [Galois](http://iss.ices.utexas.edu/?p=projects/galois) (We used version 4.0,
  the latest version as of February 2019).


\[1\]: Low-Latency Graph Streaming Using Compressed Purely-Functional Trees,
Laxman Dhulipala, Guy Blelloch, and Julian Shun
