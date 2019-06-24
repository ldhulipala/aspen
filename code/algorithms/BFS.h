#pragma once

struct BFS_F {
  uintV* parents;
  BFS_F(uintV* _parents) : parents(_parents) {}
  inline bool update (uintV s, uintV d) {
    if(parents[d] == UINT_V_MAX) { parents[d] = s; return 1; }
    else return 0;
  }
  inline bool updateAtomic (uintV s, uintV d) {
    return (pbbs::atomic_compare_and_swap(&parents[d],UINT_V_MAX,s));
  }
  inline bool cond (uintV d) { return (parents[d] == UINT_V_MAX); }
};

template <class Graph>
void BFS(Graph& G, uintV src, bool print_statistics=false) {
  timer bfs_t; bfs_t.start();
  size_t n = G.num_vertices();
  auto frontier = vertex_subset(n, src);
  timer sparse_t, dense_t, fetch_t, other_t;
  auto parents = pbbs::sequence<uintV>(n, [&] (size_t i) { return UINT_V_MAX; });
  parents[src] = src;
  size_t reachable = 0;
  while (!frontier.is_empty()) {
    reachable += frontier.size();
    // cout << frontier.size() << endl;
    auto output = G.edge_map(frontier, BFS_F(parents.begin()), sparse_t, dense_t, fetch_t, other_t, stay_dense);
    frontier.del();
    frontier = output;
  }
  frontier.del();
  if (print_statistics) {
    sparse_t.report(sparse_t.get_total(), "sparse time");
    dense_t.report(dense_t.get_total(), "dense time");
    fetch_t.report(fetch_t.get_total(), "fetch time");
    other_t.report(other_t.get_total(), "other time");
    cout << "Reachable = " << reachable << endl;
    bfs_t.report(bfs_t.stop(), "BFS time");
  }
}

template <class Graph>
void BFS_Fetch(Graph& G, uintV src, bool print_statistics=false) {
  timer bfs_t; bfs_t.start();
  size_t n = G.num_vertices();
  timer ss; ss.start();
  auto vtxs = G.fetch_all_vertices();
  if (print_statistics) {
    ss.stop(); ss.reportTotal("snapshot time");
  }

  auto frontier = vertex_subset(n, src);
  timer sparse_t, dense_t, other_t;
  auto parents = pbbs::sequence<uintV>(n, [&] (size_t i) { return UINT_V_MAX; });
  parents[src] = src;
  size_t reachable = 0;
  while (!frontier.is_empty()) {
    reachable += frontier.size();
//    cout << frontier.size() << endl;
    auto output = G.edge_map(frontier, BFS_F(parents.begin()), vtxs, sparse_t, dense_t, other_t, stay_dense);
    frontier.del();
    frontier = output;
  }
  frontier.del();
  if (print_statistics) {
    sparse_t.report(sparse_t.get_total(), "sparse time");
    dense_t.report(dense_t.get_total(), "dense time");
    other_t.report(other_t.get_total(), "other time");
    cout << "Reachable = " << reachable << endl;
    bfs_t.report(bfs_t.stop(), "BFS time");
  }
}

