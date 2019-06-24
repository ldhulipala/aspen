#pragma once

#include "vertex_subset.h"
#include "../pbbslib/seq.h"

typedef uint32_t flags;
const flags no_output = 1;
const flags dense_forward = 2;
const flags stay_dense = 4; // run dense if in dense-mode, and vs.size() > vtx_thresh
const flags run_sequential = 8; // run sequentially
const flags fine_parallel = 16; // run with a granularity of 1
const flags no_dense = 32; // don't run dense (used to compare with systems that don't implement direction optimization)
inline bool should_output(const flags& fl) { return !(fl & no_output); }

// Defines edge_map, vertex_map, adds high level traversal primitives over the
// underlying graph
template <class graph>
struct traversable_graph : private graph {
  using G = graph;
  using G::G; // imports graph's constructors
  using vertices = typename G::vertices;
  using vertex_entry = typename vertices::E;
  using Vertex_GC = typename vertices::GC;

  using Node = typename G::Node;
  using Node_GC = typename G::Node_GC; // same as Vertex_GC

  // Underlying graph implementation must define edge_struct
  using edge_struct = typename G::edge_struct;
  using vtx = pair<uintV, edge_struct>;

  // for coercing the underlying graph to an traversable_graph
  traversable_graph(graph&& m) { if (this != &m) {this->V.root = m.V.root; m.V.root = nullptr;} }
  traversable_graph() {}

  template <class F>
  auto edge_map_dense(vertex_subset& vs, F f, const flags fl=0) {
//    cout << "Running dense" << endl;
    vs.to_dense();
    size_t n = vs.n;
    auto prev = vs.d;
    size_t granularity = utils::node_limit; // (fl & fine_parallel) ? 1 : utils::node_limit;
    if (should_output(fl)) {
      auto next = pbbs::sequence<bool>(n, false);
      auto map_f = [&] (const vertex_entry& entry, size_t ind) {
        const uintV& v = entry.first;
        const auto& el = entry.second;
        auto search_f = [&] (uintV ngh) {
          if (prev[ngh] && f.update(ngh, v)) {
            next[v] = true;
          }
          return !f.cond(v);
        };
        if (f.cond(v)) {
          el.iter_elms_cond(v, search_f);
        }
      };
      G::map_vertices(map_f, fl & run_sequential, granularity);
      return vertex_subset(vs.n, next.to_array());
    } else {
      auto map_f = [&] (const vertex_entry& entry, size_t ind) {
        const uintV& v = entry.first;
        const auto& el = entry.second;
        auto search_f = [&] (uintV ngh) {
          if (prev[ngh]) {
            f.update(ngh, v);
          }
          return !f.cond(v);
        };
        if (f.cond(v)) {
          el.iter_elms_cond(v, search_f);
        }
      };
      G::map_vertices(map_f, fl & run_sequential, granularity);
      return vertex_subset(vs.n);
    }
  }

  template <class F>
  auto edge_map_dense(vertex_subset& vs, F f, pbbs::sequence<edge_struct> const &vtxs, const flags fl=0) {
//    cout << "Running dense" << endl;
    vs.to_dense();
    size_t n = vs.n;
    auto prev = vs.d;
    size_t parallel_granularity = (fl & fine_parallel) ? 1 : 1024;
    if (should_output(fl)) {
      auto next = pbbs::new_array_no_init<bool>(n);
      parallel_for(0, n, [&] (size_t i) { next[i] = 0; }, (fl & run_sequential) ? std::numeric_limits<long>::max() : 0);
      parallel_for(0, n, [&] (size_t i) {
        auto search_f = [&] (uintV ngh) {
          if (prev[ngh] && f.update(ngh, i)) {
            next[i] = true;
          }
          return !f.cond(i);
        };
        if (f.cond(i)) vtxs[i].iter_elms_cond(i, search_f);
      }, (fl & run_sequential) ? std::numeric_limits<long>::max() : parallel_granularity); // TODO: granularity
      return vertex_subset(vs.n, next);
    } else {
      parallel_for(0, n, [&] (size_t i) {
        auto search_f = [&] (uintV ngh) {
          if (prev[ngh]) {
            f.update(ngh, i);
          }
          return !f.cond(i);
        };
        if (f.cond(i)) vtxs[i].iter_elms_cond(i, search_f);
      }, (fl & run_sequential) ? std::numeric_limits<long>::max() : parallel_granularity); // TODO: granularity
      return vertex_subset(vs.n);
    }
  }

  template <class F>
  auto edge_map_dense_forward(vertex_subset& vs, F f, pbbs::sequence<edge_struct> const &vtxs, const flags fl=0) {
    // cout << "Running dense forward" << endl;
    vs.to_dense();
    size_t n = vs.n;
    if (should_output(fl)) {
      auto next = pbbs::new_array_no_init<bool>(n);
      parallel_for(0, n, [&] (size_t i) { next[i] = 0; }, (fl & run_sequential) ? std::numeric_limits<long>::max() : 0);
      parallel_for(0, n, [&] (size_t i) {
        if (vs.is_in(i)) {
          auto map_f = [&] (uintV src, uintV ngh) {
            if (f.cond(ngh) && f.updateAtomic(src, ngh)){
              next[ngh] = true;
            }
          };
          vtxs[i].map_nghs(i, map_f);
        }
      }, (fl & run_sequential) ? std::numeric_limits<long>::max() : 1); // TODO: granularity
      return vertex_subset(vs.n, next);
    } else {
      parallel_for(0, n, [&] (size_t i) {
        if (vs.is_in(i)) {
          auto map_f = [&] (uintV src, uintV ngh) {
            if (f.cond(ngh)) {
              f.updateAtomic(src, ngh);
            }
          };
          vtxs[i].map_nghs(i, map_f);
        }
      }, (fl & run_sequential) ? std::numeric_limits<long>::max() : 1); // TODO: granularity
      return vertex_subset(vs.n);
    }
  }

  template <class F>
  auto edge_map_sparse(pbbs::sequence<vtx> const &vertices, vertex_subset& vs, F f, const flags fl = 0) {
    size_t m = vertices.size();

    if (should_output(fl)) {
      auto offsets = pbbs::sequence<uintE>(m+1);
      parallel_for(0, m, [&] (size_t i) {
        auto& vv = vertices[i];
        if (get<0>(vv) != UINT_V_MAX) {
          offsets[i] = vertices[i].second.size();
        } else {
          offsets[i] = 0;
        }
      }, (fl & run_sequential) ? std::numeric_limits<long>::max() : 0); // TODO: case seq
      offsets[m] = 0;
      size_t total_deg = pbbs::scan_inplace(offsets.slice(), pbbs::addm<uintE>());
      auto nghs_u = pbbs::sequence<uintV>(total_deg);

      parallel_for(0, m, [&] (size_t i) {
        const auto& v_and_el = vertices[i];
        const uintV& v = get<0>(v_and_el);
        const auto& EL = get<1>(v_and_el);
        // map over the edgelist.
        size_t off = offsets[i];
        size_t deg = offsets[i+1] - off;
        if (deg > 0) {
          auto map_f = [&] (const uintV& ngh, size_t ind) {
            if (f.cond(ngh) && f.updateAtomic(v, ngh)) {
              nghs_u[off + ind] = ngh;
            } else {
              nghs_u[off + ind] = UINT_V_MAX;
            }
          };
          EL.map_elms(v, map_f);
        }
      }, (fl & run_sequential) ? std::numeric_limits<long>::max() : 1); // TODO: granularity

      auto p = [] (const uintV& v) { return v != UINT_V_MAX; };
      auto nghs = pbbs::filter(nghs_u, p);
      auto output_size = nghs.size();
      return vertex_subset(vs.n, output_size, nghs.to_array());
    }

    parallel_for(0, m, [&] (size_t i) {
      const auto& v_and_el = vertices[i];
      const uintV& v = get<0>(v_and_el);
      const auto& EL = get<1>(v_and_el);
      // map over the edgelist.
      if (v != UINT_V_MAX) {
        auto map_f = [&] (const uintV& ngh, size_t ind) {
          if (f.cond(ngh)) f.updateAtomic(v, ngh);
        };
        EL.map_elms(v, map_f);
      }
    }, (fl & run_sequential) ? std::numeric_limits<long>::max() : 1); // TODO: granularity
    return vertex_subset(vs.n);
  }

  template <class F>
  auto edge_map_sparse(vertex_subset& vs, F f, pbbs::sequence<uintE> const &offsets, pbbs::sequence<edge_struct> const &vtxs, const flags fl = 0) {
    size_t m = vs.size();
    vs.to_sparse();

    if (should_output(fl)) {
      size_t total_deg = offsets[vs.size()];
      auto nghs_u = pbbs::sequence<uintV>(total_deg);

      parallel_for(0, m, [&] (size_t i) {
        size_t off = offsets[i], deg = offsets[i+1] - off;
        if (deg > 0) {
          uintV v = vs.s[i];
          const auto& EL = vtxs[v];
          auto map_f = [&] (const uintV& ngh, size_t ind) {
            if (f.cond(ngh) && f.updateAtomic(v, ngh)) {
              nghs_u[off + ind] = ngh;
            } else {
              nghs_u[off + ind] = UINT_V_MAX;
            }
          };
          EL.map_elms(v, map_f);
        }
      }, (fl & run_sequential) ? std::numeric_limits<long>::max() : 1); // TODO: granularity

      auto p = [] (const uintV& v) { return v != UINT_V_MAX; };
      auto nghs = pbbs::filter(nghs_u, p);
      auto output_size = nghs.size();
      return vertex_subset(vs.n, output_size, nghs.to_array());
    }

    parallel_for(0, m, [&] (size_t i) {
      uintV v = vs.s[i];
      const auto& EL = vtxs[v];
      auto map_f = [&] (const uintV& ngh, size_t ind) {
        if (f.cond(ngh)) f.updateAtomic(v, ngh);
      };
      EL.map_elms(v, map_f);
    }, (fl & run_sequential) ? std::numeric_limits<long>::max() : 1); // TODO: granularity
    return vertex_subset(vs.n);
  }


  template <class F>
  auto edge_map(vertex_subset& vs, F f, timer& sparse_t, timer& dense_t, timer& fetch_t, timer& other_t, const flags fl=0) {
    size_t n = vs.n;
    size_t m = G::num_edges();
    size_t threshold = m/20;
    if (vs.size() == 0) { return vertex_subset(n); }

    if (fl & stay_dense && (vs.is_dense && vs.size() > (n/10))) {
      dense_t.start();
      auto ret = edge_map_dense(vs, f, fl);
      dense_t.stop();
      return ret;
    }

    pbbs::sequence<vtx> vertices(vs.size());
    fetch_t.start();
    vs.to_sparse();
    parallel_for(0, vs.size(), [&] (size_t i) {
      uintV v = vs.s[i];
      auto mv = G::find_vertex(v);
      if (mv.valid) {
        vertices[i] = make_pair(v, mv.value);
      } else {
        get<0>(vertices[i]) = UINT_V_MAX;
      }
    }, (fl & run_sequential) ? std::numeric_limits<long>::max() : 0); // TODO: case seq
    fetch_t.stop();
    size_t total_edges = 0;
    other_t.start();
    auto size_map = pbbs::sequence<size_t>(vertices.size(), [&] (size_t i) {
      const auto& vv = vertices[i];
      if (get<0>(vv) != UINT_V_MAX) {
        return vertices[i].second.size();
      } else {
        return static_cast<size_t>(0);
      }
    });
    total_edges = pbbs::reduce(size_map, pbbs::addm<size_t>());
    other_t.stop();
    if (total_edges + vs.size() > threshold) {
      dense_t.start();
      auto ret = edge_map_dense(vs, f, fl);
      dense_t.stop();
      return ret;
    } else {
      sparse_t.start();
      auto ret = edge_map_sparse(vertices, vs, f, fl);
      sparse_t.stop();
      return ret;
    }
  }

  template <class F>
  auto edge_map(vertex_subset& vs, F f, pbbs::sequence<edge_struct> const &vtxs, timer& sparse_t, timer& dense_t, timer& other_t, const flags fl=0) {
    size_t n = vs.n, m = G::num_edges(), threshold = m/20;
    if (vs.size() == 0) { return vertex_subset(n); }

    // cout << "vs.size = " << vs.size() << " is_dense = " << vs.is_dense << endl;

    if (fl & stay_dense && (vs.is_dense && vs.size() > (n/10))) {
//      cout << "stay dense " << vs.size() << endl;
      dense_t.start();
      auto ret = (fl & dense_forward) ? edge_map_dense_forward(vs, f, vtxs, fl) : edge_map_dense(vs, f, vtxs, fl);
      dense_t.stop();
      return ret;
    }

    other_t.start();
    vs.to_sparse();
    auto size_map = pbbs::sequence<uintE>(vs.size() + 1);
    parallel_for(0, vs.size(), [&] (size_t i) {
      uintV vv = vs.s[i];
      size_map[i] =  vtxs[vv].degree();
    }, (fl & run_sequential) ? std::numeric_limits<long>::max() : 0);
    size_map[vs.size()] = 0;
    size_t total_edges = pbbs::scan_inplace(size_map.slice(), pbbs::addm<uintE>());
    other_t.stop();
    if (total_edges + vs.size() > threshold && !(fl & no_dense)) {
//      cout << "total_edges.size = " << (total_edges + vs.size()) << endl;
      dense_t.start();
      auto ret = (fl & dense_forward) ? edge_map_dense_forward(vs, f, vtxs, fl) : edge_map_dense(vs, f, vtxs, fl);
      dense_t.stop();
      return ret;
    } else {
      sparse_t.start();
      auto ret = edge_map_sparse(vs, f, size_map, vtxs, fl);
      sparse_t.stop();
      return ret;
    }
  }


  pbbs::sequence<edge_struct> fetch_all_vertices() {
    timer t; t.start();
    size_t n = G::num_vertices();
    auto vtxs = pbbs::sequence<edge_struct>(n);
    auto map_f = [&] (const vertex_entry& entry, size_t ind) {
      const uintV& v = entry.first;
      const auto& el = entry.second;
      vtxs[v] = el;
    };
    G::map_vertices(map_f);
//    t.next("fetch time");
//    cout << "fetched" << endl;
    return vtxs;
  }

  traversable_graph insert_edges_batch(size_t m, tuple<uintV, uintV>* edges, bool sorted=false, bool remove_dups=false, size_t nn=std::numeric_limits<size_t>::max(), bool run_seq=false) const {
    return std::move(traversable_graph(G::insert_edges_batch(m, edges, sorted, remove_dups, nn, run_seq)));
  }

  traversable_graph insert_edges_batch(size_t vn, size_t vm, uintE* offsets, uintV* edges, size_t vertices_finished=0) const {
    return traversable_graph(G::insert_edges_batch(vn, vm, offsets, edges, vertices_finished));
  }

  traversable_graph delete_edges_batch(size_t m, tuple<uintV, uintV>* edges, bool sorted=false, bool remove_dups=false, size_t nn=std::numeric_limits<size_t>::max(), bool run_seq=false) const {
    return traversable_graph(G::delete_edges_batch(m, edges, sorted, remove_dups, nn, run_seq));
  }


public:
  using G::num_vertices;
  using G::num_edges;
  using G::find_vertex;
  using G::map_all_edges_noop;
  using G::map_all_edges;
  using G::iter_edges;
  using G::retrieve_edges;
  using G::check_edges;
  using G::size_in_bytes;
  using G::init;

  using G::get_root;
  using G::clear_root;
  using G::set_root;
  using G::del;

  using G::get_ref_cnt;
  using G::print_ref_cnt;
  using G::print_stats;
  using G::print_compression_stats;

  using G::test_intersect;
  using G::test_union;
  using G::test_unions;
};
