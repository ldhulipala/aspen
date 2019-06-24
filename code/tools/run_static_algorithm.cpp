#include "../graph/api.h"
#include "../algorithms/BFS.h"
#include "../algorithms/BC.h"
#include "../algorithms/LDD.h"
#include "../algorithms/k-Hop.h"
#include "../algorithms/mutual_friends.h"
#include "../algorithms/MIS.h"
#include "../algorithms/Nibble.h"
#include "../trees/utils.h"

#include <cstring>
#include <chrono>
#include <thread>
#include <cmath>
#include <random>

string test_name[] = {
    "BFS",
    "BC",
    "MIS",
    "KHOP",
    "NIBBLE",
};

template <class G>
double test_bfs(G& GA, commandLine& P) {
  bool no_snapshot = P.getOptionValue("-noflatsnap");
  bool print_stats = P.getOptionValue("-stats");
  long src = P.getOptionLongValue("-src",-1);
  if (src == -1) {
    std::cout << "Please specify a source vertex to run the BFS from using -src" << std::endl;
    exit(0);
  }
  std::cout << "Running BFS from source = " << src << std::endl;
  timer bfst; bfst.start();
  if (no_snapshot) {
    BFS(GA, src, print_stats);
  } else {
    BFS_Fetch(GA, src, print_stats);
  }
  bfst.stop();
  return bfst.get_total();
}

template <class G>
double test_bc(G& GA, commandLine& P) {
  size_t src = static_cast<size_t>(P.getOptionLongValue("-src",0));
  std::cout << "Running BFS from source = " << src << std::endl;
  timer bfst; bfst.start();
  bool use_dense_forward = P.getOptionValue("-df");
  bool print_stats = P.getOptionValue("-stats");
  auto deps = BC(GA, src, use_dense_forward, print_stats);
  bfst.stop();
  return bfst.get_total();
}

template <class G>
double test_khop(G& GA, commandLine& P) {
  size_t num_sources = static_cast<size_t>(P.getOptionLongValue("-nsrc",40));
  std::cout << "Running k-hop: with " << num_sources << " sources" << std::endl;
  size_t k = P.getOptionLongValue("-k",2);
  size_t n = GA.num_vertices();

  auto r = pbbs::random();
  timer tmr; tmr.start();
  size_t i=0;
  // The BS is really a function of how much memory you have and the
  // structure of the input graph. The graphs we test on are power-law, and so a
  // 2-hop query can easily cover a sizable fraction of the vertices, which
  // causes memory-issues on large graphs.
  long default_bsize = (GA.num_vertices() > 100000000) ? 1 : 144;
  size_t b_size = P.getOptionLongValue("-BS", default_bsize);
  bool par = P.getOptionValue("-par");
//  size_t total_hop = 0; // Uncomment and run with -BS = 1 to compute avg 2hop size.
  while (i < num_sources) {
    size_t end = std::min(i + b_size, num_sources);
    parallel_for(i, end, [&] (size_t j) {
      uintV src = r.ith_rand(j) % n;
      size_t hop = twoHop_par(GA, src);
//      total_hop += hop;
    }, 1);
    i += b_size;
  }
  tmr.stop();
//  std::cout << "avg 2-hop = " << ((total_hop*static_cast<double>(1.0)) / num_sources) << std::endl;
  return (tmr.get_total() / num_sources);
}


template <class G>
double test_mis(G& GA, commandLine& P) {
  bool print_stats = P.getOptionValue("-stats");
  timer t; t.start();
  MIS(GA, print_stats);
  t.stop();
  return t.get_total();
}

template <class G>
double test_nibble(G& GA, commandLine& P) {
  size_t num_sources = static_cast<size_t>(P.getOptionLongValue("-nsrc",1024));
  double epsilon = P.getOptionDoubleValue("-e",0.000001);
  size_t T = static_cast<size_t>(P.getOptionLongValue("-T",10));
  std::cout << std::scientific << setprecision(3) << "Running Nibble,  epsilon = " << epsilon << " T = " << T << std::endl;

  size_t n = GA.num_vertices();
  auto r = pbbs::random();
  timer t; t.start();
  size_t i = 0;
  size_t b_size = P.getOptionLongValue("-BS", num_sources);
  while (i < num_sources) {
    size_t end = std::min(i + b_size, num_sources);
    parallel_for(i, end, [&] (size_t j) {
      uintV src = r.ith_rand(j) % n;
      NibbleSerial(GA, src, epsilon, T);
    }, 1);
    i += b_size;
  }
  t.stop();
  return (t.get_total() / num_sources);
}

template <class Graph>
double execute(Graph& G, commandLine& P, string testname) {
  if (testname == "BFS") {
    return test_bfs(G, P);
  } else if (testname == "BC") {
    return test_bc(G, P);
  } else if (testname == "KHOP") {
    return test_khop(G, P);
  } else if (testname == "MIS") {
    return test_mis(G, P);
  } else if (testname == "NIBBLE") {
    return test_nibble(G, P);
  } else {
    std::cout << "Unknown test: " << testname << ". Quitting." << std::endl;
    exit(0);
  }
}

void run_algorithm(commandLine& P) {
  auto test_id = P.getOptionValue("-t", "BFS");
  size_t threads = num_workers();

  auto VG = initialize_treeplus_graph(P);
  // Run the algorithm on it
  size_t rounds = P.getOptionLongValue("-rounds", 4);
  double total_time = 0.0;
  int ok;
  if (P.getOptionValue("-drop")) {
    std::cout << "Enter after dropping caches." << std::endl;
    cin >> ok;
  }
  for (size_t i=0; i<rounds; i++) {
    auto S = VG.acquire_version();
    double tm = execute(S.graph, P, test_id);
    std::cout << "RESULT"  << fixed << setprecision(6)
  	 << "\ttest=" << test_id
  	 << "\ttime=" << tm
  	 << "\titeration=" << i
  	 << "\tp=" << threads << std::endl;
    total_time += tm;
    VG.release_version(std::move(S));
  }
  std::cout << "RESULT (AVG)" << fixed << setprecision(6)
  	<< "\ttest=" << test_id
  	<< "\ttime=" << (total_time / rounds)
  	<< "\tp=" << threads << std::endl;
}

int main(int argc, char** argv) {
  std::cout << "Running Aspen using " << num_workers() << " threads." << std::endl;
  commandLine P(argc, argv, "./test_parallel [-t testname -r rounds -f file -m (mmap)]");
  run_algorithm(P);
}
