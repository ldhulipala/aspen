#pragma once

#include "../lib_extensions/sparse_table.h"
#include "../lib_extensions/sequentialHT.h"
#include <unordered_set>

template <class T>
struct Traverse_Table_F {
  T& table;
  Traverse_Table_F(T& _table) : table(_table) {}
  inline bool update (uintV s, uintV d) {
    table.insert(make_tuple(d, pbbs::empty())); return 1;
  }
  inline bool updateAtomic (uintV s, uintV d) {
    return table.insert(make_tuple(d, pbbs::empty()));
  }
  inline bool cond (uintV d) { return !table.contains(d); }
};
template <class T>
auto make_traverse_table_f(T& table) { return Traverse_Table_F<T>(table); }

struct Traverse_Dense_F {
  bool* bits;
  Traverse_Dense_F(bool* _bits) : bits(_bits) {}
  inline bool update (uintV s, uintV d) {
    if(bits[d] == false) { bits[d] = true; return 1; }
    else return 0;
  }
  inline bool updateAtomic (uintV s, uintV d) {
    return (pbbs::atomic_compare_and_swap(&bits[d],false,true));
  }
  inline bool cond (uintV d) { return (bits[d] == false); }
};

template <class Graph>
size_t kHop(Graph& G, uintV src, size_t k, bool seq=false, bool print_stats=false) {
//  cout << "in kHop" << endl;
  size_t n = G.num_vertices(); size_t m = G.num_edges();
//  cout << "n = " << n << " m = " << m << endl;
  double avg_degree = static_cast<double>(m) / n;
  auto get_degree = [&] (uintV i) {
    const auto& mv = G.find_vertex(i);
    if (mv.valid) {
      return mv.value.degree();
    }
    return static_cast<size_t>(0);
  };
  size_t src_degree = get_degree(src);
//  cout << "src_degree = " << src_degree << endl;
  if (k == 1) { return src_degree; }
  if (src_degree == 0) { return 0; }

  size_t max_table_size = (size_t)(1L << size_t(pbbs::log2_up<size_t>((size_t)(1.1*n))));
//  cout << "max_table_size = " << max_table_size << endl;
  size_t est_neighbors = std::min(max_table_size, (size_t)(src_degree*(pow(avg_degree, std::max(k-1, (size_t)0)))));
//  cout << "est_neighbors = " << est_neighbors << endl;

  auto visited = sparse_table<uintV, pbbs::empty>(est_neighbors,
             make_tuple(UINT_E_MAX, pbbs::empty()));

  auto frontier = vertex_subset(n, src);
  timer sparse_t, dense_t, fetch_t, other_t;
  size_t reachable = 0; size_t rd = 0;
  bool* bits = nullptr; bool dense=false;
  auto fl = (seq) ? pbbs::fl_sequential : 0;

  while (!frontier.is_empty() && rd <= k) {
    reachable += frontier.size();
    if (rd > 0) {
      frontier.to_sparse();
      auto deg_im = pbbs::delayed_seq<size_t>(frontier.size(), [&] (size_t i) { return get_degree(frontier.s[i]); });
      size_t incoming_degree = pbbs::reduce(deg_im, pbbs::addm<size_t>(), fl);
      if (incoming_degree > n/4) {
        dense=true;
        bits = pbbs::new_array_no_init<bool>(n);
        for (size_t i=0; i<n; i++) { bits[i] = 0; }
        for(size_t i=0; i<visited.m; i++) {
          if (get<0>(visited.table[i]) != visited.empty_key) {
            bits[get<0>(visited.table[i])] = true;
          }
        };
      }

      if (!dense && visited.m < max_table_size && visited.m < (1.1)*(incoming_degree + reachable)) { // resize
        size_t new_size = std::min(n, incoming_degree + reachable);
        auto new_v = sparse_table<uintV, pbbs::empty>(new_size, make_tuple(UINT_E_MAX, pbbs::empty()));
        for (size_t i=0; i<visited.m; i++) {
          if (get<0>(visited.table[i]) != visited.empty_key) {
            new_v.insert(visited.table[i]);
          }
        }
        visited.del();
        visited = new_v;
      }
    }

    vertex_subset output(n);
    flags fl = 0;
    if (k == (rd-1)) fl |= no_output;
    if (seq) fl |= run_sequential;
    if (dense) {
      output = G.edge_map(frontier, Traverse_Dense_F(bits), sparse_t, dense_t, fetch_t, other_t, fl);
    } else {
      output = G.edge_map(frontier, make_traverse_table_f(visited), sparse_t, dense_t, fetch_t, other_t, fl);
    }
    frontier.del();
    frontier = output;
    rd++;
  }
  frontier.del(); visited.del();
  if (dense) { pbbs::free_array(bits); }
  return reachable;
}

template <class Graph>
size_t twoHop(Graph& G, uintV src, size_t k, size_t degree_threshold, bool seq=false, bool print_stats=false) {
  cout << endl;
  cout << "started twoHop " << src << endl;
  timer t; t.start();
  size_t n = G.num_vertices(); size_t m = G.num_edges();
  double avg_degree = static_cast<double>(m) / n;
  auto get_degree = [&] (uintV i) {
    const auto& mv = G.find_vertex(i);
    if (mv.valid) {
      return mv.value.degree();
    }
    return static_cast<size_t>(0);
  };

  size_t src_degree = get_degree(src);
  if (src_degree == 0) { return 0; }

  size_t max_table_size = (size_t)(1L << size_t(pbbs::log2_up<size_t>((size_t)(n/5))));
  size_t est_neighbors = std::min(max_table_size, (size_t)(src_degree*avg_degree));

  auto visited = sparse_table<uintV, pbbs::empty>(est_neighbors,
             make_tuple(UINT_E_MAX, pbbs::empty()));

  auto frontier = vertex_subset(n, src);
  timer sparse_t, dense_t, fetch_t, other_t;
  size_t reachable = 0; size_t rd = 0;

  reachable += frontier.size();
//  auto fl = run_sequential;
  auto one_hop = G.edge_map(frontier, make_traverse_table_f(visited), sparse_t, dense_t, fetch_t, other_t);
  cout << "one_hop.size = " << one_hop.size() << endl;

  auto vtxs = pbbs::sequence<typename Graph::edge_struct>(one_hop.size(), [&] (size_t i) {
    uintE v = one_hop.s[i];
    return G.find_vertex(v).value;
  });

  one_hop.to_sparse();
  timer tt; tt.start();
  size_t incoming_degree = 0;
  auto ss = pbbs::delayed_seq<size_t>(vtxs.size(), [&] (size_t i) { return static_cast<size_t>(vtxs[i].degree()); });
  incoming_degree = pbbs::reduce(ss, pbbs::addm<size_t>());
  tt.stop(); // tt.reportTotal("degree computation time");
//  cout << "incoming degree = " << incoming_degree << endl;

  if (one_hop.size() > 0) {
    if (incoming_degree < degree_threshold) {

      visited.resize_if_necessary_par(incoming_degree);
//      cout << "sparse" << endl;
      // sparse
      parallel_for(0, one_hop.size(), [&] (size_t i) {
        uintV v = one_hop.s[i];
        const auto& mv = vtxs[i];
        auto degree = mv.degree();
//        visited.resize_if_necessary(degree);
//        cout << "v = " << v << " degree = " << degree << endl;
        auto iter_f = [&] (uintV src, uintV ngh) {
          reachable += visited.insert(std::make_tuple(ngh, pbbs::empty()));
//          return false;
        };
        mv.map_nghs(v, iter_f);
      }, 1);
      auto seq = pbbs::delayed_seq<size_t>(visited.m, [&] (size_t i) {
        return static_cast<size_t>(std::get<0>(visited.table[i]) != visited.empty_key);
      });
      reachable = pbbs::reduce(seq, pbbs::addm<size_t>());
    } else {
//      cout << "dense" << endl;
      timer ttt; ttt.start();
      auto d = pbbs::sequence<bool>(n, false);
      d[src] = true;
      parallel_for (0, one_hop.size(), [&] (size_t i) {
        d[one_hop.s[i]] = true;
      });
      parallel_for(0, one_hop.size(), [&] (size_t i) {
        uintE v = one_hop.s[i];
        const auto& mv = vtxs[i];
        auto map_f = [&] (const uintV& ngh, size_t ind) {
          if (!d[ngh]) { d[ngh] = true; }
        };
        mv.map_elms(v, map_f);
      }, 1);
      auto delayed_d = pbbs::delayed_seq<size_t>(d.size(), [&] (size_t i) { return static_cast<size_t>(d[i]); });
      reachable = pbbs::reduce(delayed_d, pbbs::addm<size_t>());
      ttt.stop(); ttt.reportTotal("dense time");
    }
  }
  frontier.del(); visited.del();
//  cout << "finished twoHop " << src << " reach = " << reachable << endl;
  t.stop(); t.reportTotal("time");
  return reachable;
}

template <class Graph>
size_t twoHop_seq(Graph& G, uintV src) {
  cout << "2hop from " << src << endl;
  std::unordered_set <uintV> q(4096); // reasonable size
  timer t; t.start();
  q.insert(src);
  const auto& v = G.find_vertex(src).value;

  auto map_f = [&] (uintV ngh) {
    q.insert(ngh);
    return false;
  };
  v.iter_elms_cond(src, map_f);

  auto q_copy = q; // copy constructor

  for (const auto& ngh_id : q_copy) {
    // edge_struct
    const auto& ngh = G.find_vertex(ngh_id).value;
    ngh.iter_elms_cond(ngh_id, map_f);
  }
  t.stop(); t.reportTotal("2hop time");
  cout << q.size() << endl << endl;
  cout << "worker = " << worker_id() << endl;
  return q.size();
}


template <class Graph>
size_t twoHop_par(Graph& G, uintV src) {
//  cout << "2hop from " << src << endl;
  const auto& v = G.find_vertex(src).value;

  pbbs::sequence<bool> hop(G.num_vertices(), false);
  timer t; t.start();
  hop[src] = true;

  auto snd_map_f = [&] (uintV ngh, size_t ind) {
    if (!hop[ngh]) {
      hop[ngh] = true;
    }
  };

  auto map_f = [&] (uintV ngh_id, size_t ind) {
    if (!hop[ngh_id]) {
      hop[ngh_id] = true;
    }
    const auto& ngh = G.find_vertex(ngh_id).value;
    ngh.map_elms(ngh_id, snd_map_f);
  };
  v.map_elms(src, map_f);

  t.stop(); t.reportTotal("2hop time");
  cout << "worker = " << worker_id() << endl;
  auto del_seq = pbbs::delayed_seq<uintV>(hop.size(), [&] (size_t i) { return static_cast<size_t>(hop[i]); });
  size_t hop_size = pbbs::reduce(del_seq, pbbs::addm<uintV>());
  cout << hop_size << endl;
  return hop_size;
}

