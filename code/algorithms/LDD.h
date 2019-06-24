#pragma once

#include "../pbbslib/random_shuffle.h"
#include "../pbbslib/monoid.h"

#include <cmath>

size_t total_rounds(size_t n, double beta) {
  return min<uintV>(n+1,2+ceil(log(n)/beta));
}

// Shifts[i] is the start of the vertices to take on round i
auto generate_shifts(size_t n, double beta) {
  // Create (ln n)/beta levels
  uintV last_round = total_rounds(n, beta);
  auto shifts = pbbs::sequence<uintV>(last_round+1);
  parallel_for(0, last_round, [&] (size_t i) {
    shifts[i] = floor(exp(i*beta));
  });
  shifts[last_round] = 0;
  pbbs::scan_inplace(shifts.slice(), pbbs::addm<uintV>());
  return shifts;
}

// Edgemap struct for LDD.
struct LDD_F {
  uintV *cluster_ids;

  LDD_F(uintV* _cluster_ids) : cluster_ids(_cluster_ids) { }

  inline bool update(const uintV& s, const uintV& d) {
    cluster_ids[d] = cluster_ids[s];
    return true;
  }

  inline bool updateAtomic(const uintV& s, const uintV& d) {
    return pbbs::atomic_compare_and_swap(&cluster_ids[d], UINT_V_MAX, cluster_ids[s]);
  }

  inline bool cond(uintV d) {
    return cluster_ids[d] == UINT_V_MAX;
  }
};

template <class Graph>
auto LDD(Graph& GA, double beta, bool permute=true, bool print_stats=false) {
  size_t n = GA.num_vertices();
  timer lddt; lddt.start();

  uintV* vertex_perm;
  if (permute) {
    auto perm = pbbs::random_permutation<uintV>(n);
    vertex_perm = perm.to_array();
  }
  auto shifts = generate_shifts(n, beta);
  auto cluster_ids = pbbs::sequence<uintV>(n);
  parallel_for(0, n, [&] (size_t i) { cluster_ids[i] = UINT_V_MAX; });

  timer sparse_t, dense_t, fetch_t, other_t;
  size_t last_round = total_rounds(n, beta);
  size_t round = 0, num_visited = 0;
  vertex_subset frontier(n); // Initially empty
  size_t num_added = 0;
  while (num_visited < n) {
    assert(round < last_round);
    size_t start = shifts[round];
    size_t end = min(static_cast<size_t>(shifts[round+1]), n);
    size_t num_to_add = end - start;
    if (num_to_add > 0) {
      assert((num_added + num_to_add) <= n);
      auto candidates = pbbs::delayed_seq<uintV>(num_to_add, [&] (size_t i) {
        if (permute) return vertex_perm[num_added + i];
        else return static_cast<uintV>(num_added + i);
      });
      auto pred = [&] (uintV v) { return cluster_ids[v] == UINT_V_MAX; };
      auto new_centers = pbbs::filter(candidates, pred);
      add_to_vsubset(frontier, new_centers.begin(), new_centers.size());
      parallel_for(0, new_centers.size(), [&] (size_t i) {
        cluster_ids[new_centers[i]] = new_centers[i];
      });
      num_added += num_to_add;
    }

    num_visited += frontier.size();
    if (num_visited >= n) break;

    auto ldd_f = LDD_F(cluster_ids.begin());
    auto next_frontier = GA.edge_map(frontier, ldd_f, sparse_t, dense_t, fetch_t, other_t);
    frontier.del();
    frontier = next_frontier;

    round++;
  }
  return std::move(cluster_ids);
}
