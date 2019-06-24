#pragma once

#include "../lib_extensions/sparseSet.h"
#include <unordered_map>

#define LOAD_FACTOR 1.1

using wgh_t = float;
using vertex_weight_set = sparseAdditiveSet<uintV, wgh_t>;

template <class D>
struct Nibble_F {
  vertex_weight_set p, new_p;
  D& get_degree;
  Nibble_F(vertex_weight_set &_p, vertex_weight_set &_new_p, D& _D) :
    p(_p), new_p(_new_p), get_degree(_D) {}
  inline bool update(uintV s, uintV d) { // update function applies PageRank equation
    return new_p.insert(make_pair(d,get<1>(p.find(s))/(2*get_degree(s))));
  }
  inline bool updateAtomic (uintV s, uintV d) {
    return new_p.insert(make_pair(d,get<1>(p.find(s))/(2*get_degree(s))));
  }
  inline bool cond (uintV d) { return true; }
};

template <class D>
auto make_nibble_f(vertex_weight_set& p, vertex_weight_set& new_p, D& d) {
  return Nibble_F<D>(p, new_p, d);
}


// Implements the Parallel Nibble algorithm as described in "Parallel Local
// Graph Clustering" by Shun et al. (VLDB'16)
template <class Graph>
void NibbleParallel(Graph& G,
               const uintV start,
               const double epsilon=0.000000001,
               const long T=10) {
  size_t n = G.num_vertices();
  timer sparse_t, dense_t, fetch_t, other_t;

  using ACLentry = tuple<uintV, wgh_t>;
  ACLentry empty = make_tuple(std::numeric_limits<uintV>::max(), (wgh_t)0.0);
  vertex_weight_set p = vertex_weight_set(2, empty, LOAD_FACTOR);
  p.insert(make_pair(start,1.0));
  vertex_subset Frontier(n,start);

  // Each get_degree calls increases work by a log(n) factor. This can be made
  // O(1) by getting a snapshot of the graph, but it's not worth it for local
  // algorithms that do little-o(n) work. We can also save constant factors by
  // memoizing the degrees.
  auto get_degree = [&] (uintV i) {
    const auto& mv = G.find_vertex(i);
    if (mv.valid) {
      return mv.value.degree();
    }
    return static_cast<size_t>(0);
  };
  auto sum = [&] (size_t l, size_t r) { return l + r; };

  long iter = 0, total_pushes = 0;
  while(Frontier.size() > 0 && iter++ < T) {
    total_pushes += Frontier.size();

    size_t total_degree = Frontier.map_reduce(get_degree, sum, (size_t)0);
    size_t pCount = p.count();
    vertex_weight_set new_p =
      vertex_weight_set(max(100UL, min((size_t)n,total_degree+pCount)), empty, LOAD_FACTOR); // make bigger table

    // Half the weight stays on v \in p.
    vertex_map(Frontier, [&] (uintV i) {
      new_p.insert(make_pair(i, get<1>(p.find(i))/2));
    });

    // Half the weight gets spread to neighbors.
    G.edge_map(Frontier, make_nibble_f(p, new_p, get_degree),
               sparse_t, dense_t, fetch_t, other_t, no_output);
    p.del(); p = new_p;

    auto vals = p.entries([&] (ACLentry val) {
        return get<1>(val) >= get_degree(get<0>(val))*epsilon; });
    uintV* Active = pbbs::new_array_no_init<uintV>(vals.size());
    parallel_for(0, vals.size(), [&] (size_t i) { Active[i] = get<0>(vals[i]); });
    Frontier.del();
    Frontier = vertex_subset(n,vals.size(),Active);
  }
  cout << "num iterations = " << iter-1 << " total pushes = " << total_pushes << endl;
}


// This is a serial implementation of the Nibble algorithm in "A Local
// Clustering Algorithm for Massive Graphs and its Application to
// Nearly Linear Time Graph Partitioning" by Daniel Spielman and
// Shang-Hua Teng, Siam J. Comput. 2013. Based on the implementation from
// "Parallel Local Graph Clustering" by Shun et al. (VLDB'16)
template <class Graph>
size_t NibbleSerial(Graph& G,
                  const uintV start,
                  const double epsilon=0.000001,
                  const size_t T=10,
                  bool print_stats=false) {
  unordered_map <uintV,double> q;
  q[start] = 1.0; //starting vertex
  long totalPushes = 0;
  unordered_map <uintV,typename Graph::edge_struct> cache;

  for(size_t i=0; i<T; i++) {
    unordered_map <uintV,double> q_next;
    for (auto it : q) {
      uintV v = it.first;
      double q = it.second;
      auto ret = cache.find(v);
      if (ret == cache.end()) {
        cache[v] = G.find_vertex(v).value;
        ret = cache.find(v);
      }
      const auto& V = ret->second;
      uintV d = V.degree();
      if(q > d*epsilon) {
      	totalPushes++;
      	q_next[v] += q/2;
      	double nghMass = q/(2*d);
        auto map_f = [&] (uintV ngh) {
          q_next[ngh] += nghMass;
          return false;
        };
        V.iter_elms_cond(v, map_f);
      }
    }
    if(q_next.size() == 0) break;
    q = q_next;
  }
  return totalPushes;
}


