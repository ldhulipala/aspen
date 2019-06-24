#pragma once

#include <vector>

typedef double fType;

  template <class S, class V>
  struct BC_F {
    S const& Scores;
    V const& Visited;
    BC_F(S const& _Scores, V const& _Visited) : Scores(_Scores), Visited(_Visited) {}
    inline bool update(const uintE& s, const uintE& d) {
      fType oldV = Scores[d];
      Scores[d] += Scores[s];
      return oldV == 0.0;
    }
    inline bool updateAtomic (const uintE& s, const uintE& d) {
      fType to_add = Scores[s];
      fType o_val = pbbs::fetch_and_add(&Scores[d],to_add);
      return (o_val == 0);
    }
    inline bool cond (uintE d) { return Visited[d] == 0; }
  };

  template <class S, class V>
  auto make_bc_f(S const& scores, V const& visited) {
    return BC_F<S, V>(scores, visited);
  }

  // Vertex map function to mark visited vertex_subset
  template <class V>
  struct BC_Vertex_F {
    V const& Visited;
    BC_Vertex_F(V const& _Visited) : Visited(_Visited) {}
    inline bool operator() (uintE i) {
      Visited[i] = 1;
      return 1;
    }
  };

  template <class V>
  auto make_bc_vertex_f(V const& visited) {
    return BC_Vertex_F<V>(visited);
  }

  // Vertex map function (used on backwards phase) to mark visited vertex_subset
  // and add to Dependencies score
  template <class V, class D>
  struct BC_Back_Vertex_F {
    V const& Visited;
    D const& Dependencies;
    D const& NumPaths;
    BC_Back_Vertex_F(V const& _Visited, D const& _Dependencies, D const& _NumPaths) :
      Visited(_Visited), Dependencies(_Dependencies), NumPaths(_NumPaths) {}
    inline bool operator() (uintE i) {
      Visited[i] = 1;
      Dependencies[i] += NumPaths[i];
      return 1;
    }
  };

  template <class V, class D>
  auto make_bc_back_vertex_f(V const& visited, D const& dependencies, D const& num_paths) {
    return BC_Back_Vertex_F<V, D>(visited, dependencies, num_paths);
  }

  template <class Graph>
  auto BC(Graph& G, const uintE& start, bool use_dense_forward=false, bool print_stats=false) {
    size_t n = G.num_vertices();
    auto vtxs = G.fetch_all_vertices();

    auto NumPaths = pbbs::sequence<fType>(n, [] (size_t i) { return 0.0; });
    auto Visited = pbbs::sequence<bool>(n, [] (size_t i) { return 0; });
    Visited[start] = 1; NumPaths[start] = 1.0;

    vertex_subset Frontier(n,start);

    vector<vertex_subset> Levels;

    flags fl = stay_dense | fine_parallel | ((use_dense_forward) ? dense_forward : 0);

    timer sparse_t, dense_t, other_t;
    long round = 0;
    while (!Frontier.is_empty()) {
      round++;
      // cout << Frontier.size() << endl;
      vertex_subset output = G.edge_map(Frontier, make_bc_f(NumPaths,Visited), vtxs, sparse_t, dense_t, other_t, fl);
      vertex_map(output, make_bc_vertex_f(Visited)); // mark visited
      Levels.push_back(Frontier); // save frontier
      Frontier = output;
    }
    Levels.push_back(Frontier);


    auto Dependencies = pbbs::sequence<fType>(n, [] (size_t i) { return 0.0; });

    // Invert numpaths
    parallel_for(0, n, [&] (size_t i) { NumPaths[i] = 1/NumPaths[i]; });

    Levels[round].del();
    parallel_for(0, n, [&] (size_t i) { Visited[i] = 0; });
    Frontier = Levels[round-1];
    vertex_map(Frontier,make_bc_back_vertex_f(Visited,Dependencies,NumPaths));

    for(long r=round-2;r>=0;r--) {
      G.edge_map(Frontier, make_bc_f(Dependencies,Visited), vtxs, sparse_t, dense_t, other_t, no_output | fl);
      Frontier.del();
      Frontier = Levels[r];
      vertex_map(Frontier,make_bc_back_vertex_f(Visited,Dependencies,NumPaths));
    }

    Frontier.del();

    // Update dependencies scores
    parallel_for(0, n, [&] (size_t i) {
      Dependencies[i] = (Dependencies[i]-NumPaths[i])/NumPaths[i];
    });
    if (print_stats) {
      sparse_t.report(sparse_t.get_total(), "sparse time");
      dense_t.report(dense_t.get_total(), "dense time");
      other_t.report(other_t.get_total(), "other time");
    }
    return std::move(Dependencies);
  }

