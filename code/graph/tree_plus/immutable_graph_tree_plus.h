#pragma once

#include "../../common/types.h"
#include "../../common/IO.h"
#include "../../pbbslib/seq.h"
#include "../../pbbslib/sample_sort.h"
#include "../../pbbslib/integer_sort.h"
#include "../../pbbslib/merge_sort.h"
#include "../../trees/pam.h"
#include "tree_plus.h"

#include <limits>


static double format_gb(size_t bytes) {
  double gb = bytes;
  gb /= 1024.0;
  gb /= 1024.0;
  gb /= 1024.0;
  return gb;
}

struct sym_immutable_graph_tree_plus {

  using edge_struct = tree_plus::treeplus;

  struct vertex_entry {
    using key_t = uintV;
    using val_t = edge_struct;
    static bool comp(const key_t& a, const key_t& b) { return a < b; }
    using aug_t = uintE;
    static aug_t get_empty() { return 0; }
    static aug_t from_entry(const key_t& k, const val_t& v) { return v.size(); }
    static aug_t combine(const aug_t& a, const aug_t& b) { return a + b; }
    using entry_t = std::pair<key_t, val_t>;

    static entry_t copy_entry(const entry_t& e) {
//      cout << "Copied entry: " << "key = " << e.first << " plus size = " << lists::node_size(plus) << " root = " << ((size_t)e.second.root) << " plus = " << ((size_t)plus) << endl;
      auto plus = lists::copy_node(e.second.plus); // bumps ref-cnt
      auto root = tree_plus::Tree_GC::inc(e.second.root); // bumps ref-cnt
      return make_pair(e.first, edge_struct(plus, root));
    }

    static void del(entry_t& e) {
//      cout << "Deleting vertex entry" << " key = " << e.first << " plus size = " << lists::node_size(e.second.plus) << " root = " << ((size_t)e.second.root) << endl;
      if (e.second.plus) {
        lists::deallocate(e.second.plus);
        e.second.plus = nullptr;
      }
      if (e.second.root) {
        auto T = tree_plus::edge_list();
        T.root = e.second.root;
        e.second.root = nullptr;
      }
    }
  };
  using vertices = aug_map<vertex_entry>;
  using vertices_tree = typename vertices::Tree;
  using vertices_GC = typename vertices::GC;

  // A graph just stores a vertex_tree.
  vertices V;

  size_t num_vertices() const {
    size_t n = V.size();
    auto last_vtx = V.select(n-1);
    if (n > 0) {
      assert(last_vtx.valid);
      return last_vtx.value.first + 1;
    } else {
      return 0;
    }
  }

  size_t num_edges() const { return V.aug_val(); }

  size_t get_ref_cnt() {
    if (V.root) {
      return V.root->ref_cnt;
    }
    return static_cast<size_t>(0);
  }
  void print_ref_cnt() { cout << "cnt = " << get_ref_cnt() << endl; }

  bool contains_vertex(uintV v) { return V.contains(v); }
  // returns node*
  inline auto find_vertex(uintV v) { return V.find(v); }

  using Node = typename vertices::node;
  using Node_GC = typename vertices::GC;

  Node* get_root() const {
    return V.root;
  }
  void clear_root() { //tantamount to deleting the graph
    V.root = nullptr;
  }
  void set_root(Node* node) {
    assert(V.root == nullptr);
    V.root = node; // TODO
  }

  template <class F>
  void map_vertices(F map_f, bool run_seq=false, size_t granularity=utils::node_limit) const { V.map_elms(map_f, run_seq, granularity); }

  template <class F>
  void map_vertices_no_index(F map_f) {  V.map_elms_no_index(map_f); }

  template <class T, class M, class R>
  T map_reduce(const M& m, const R& r, const T& id) {
    return V.map_reducef(m, r, id);
  }

  template <class F>
  vertices filter(F& f) {
    return vertices::filter(V, f);
  }

  void map_all_edges_noop() {
    timer mapt; mapt.start();
    auto map_f = [&] (const vertex_entry::entry_t& E, size_t ind) {
      const uintV& v = E.first;
      const auto& EL = E.second; // incs ref-ct
      auto map_edges_f = [&] (const uintV& v, size_t ind) {
        // noop
      };
      EL.map_elms(v, map_edges_f);
    };
    map_vertices(map_f);
//    mapt.next("Map all edges noop time");
  }

  template <class F>
  void map_all_edges(F& f) const {
    timer mapt; mapt.start();
    auto map_f = [&] (const vertex_entry::entry_t& E, size_t ind) {
      const uintV& u = E.first;
      const auto& EL = E.second; // incs ref-ct
      auto map_edges_f = [&] (const uintV& v, size_t ind) {
        f(u, v);
      };
      EL.map_elms(u, map_edges_f);
    };
    map_vertices(map_f);
//    mapt.next("Map all edges noop time");
  }

  size_t size_in_bytes() {
    size_t n = num_vertices();
    cout << "n = " << n << endl;
    auto live_vertices = pbbs::sequence<size_t>(n, [&] (size_t i) { return static_cast<size_t>(0); });
    auto size_seq = pbbs::sequence<size_t>(n, [&] (size_t i) { return static_cast<size_t>(0); });
    auto num_el_nodes = pbbs::sequence<size_t>(n, [&] (size_t i) { return static_cast<size_t>(0); });
    auto map_f = [&] (const vertex_entry::entry_t& E, size_t ind) {
      const uintV& v = E.first;
      const auto& EL = E.second; // incs ref-ct
      size_t el_size = EL.size_in_bytes(v);
      size_t el_nodes = EL.edge_tree_nodes();
      size_seq[v] = el_size;
      live_vertices[v] = 1;
      num_el_nodes[v] = el_nodes;
      EL.check_consistency(v);
      size_t ref_cts = EL.check_ref_cts(v);
      if (v == 489431058) {
        cout << "Structure checked, v = " << v << endl;
        EL.print_structure(v);
      }
      assert(ref_cts == el_nodes);
    };
    map_vertices(map_f);

    size_t edge_list_bytes = pbbs::reduce(size_seq, pbbs::addm<size_t>());
    size_t total_vertices = pbbs::reduce(live_vertices, pbbs::addm<size_t>());
    size_t total_el_nodes = pbbs::reduce(num_el_nodes, pbbs::addm<size_t>());
    cout << "Total edge-list bytes = " << edge_list_bytes << endl;
    cout << "Total edge-list nodes = " << total_el_nodes << endl;
    cout << "Total vertices = " << total_vertices << endl;
    return edge_list_bytes + total_vertices*56 + total_el_nodes*48;
  }

  void check_edges() {
    size_t n = num_vertices();
    timer mapt; mapt.start();
    auto map_f = [&] (const vertex_entry::entry_t& E, size_t ind) {
      const uintV& v = E.first;
      const auto& EL = E.second; // incs ref-ct
      auto map_edges_f = [&] (const uintV& v, size_t ind) {
        assert(v < n);
        // noop
      };
      EL.map_elms(v, map_edges_f);
    };
    map_vertices(map_f);
    mapt.next("Map all edges noop time");
  }

  void iter_edges() {
    size_t n = num_vertices();
    timer mapt; mapt.start();
    auto map_f = [&] (const vertex_entry::entry_t& E, size_t ind) {
      const uintV& v = E.first;
      const auto& EL = E.second; // incs ref-ct
      size_t ctr = 0;
      auto map_edges_f = [&] (const uintV& v) {
        assert(v < n);
        ctr++;
        return false;
      };
      EL.iter_elms_cond(v, map_edges_f);
      size_t deg = EL.degree();
      assert(ctr == deg);
    };
    map_vertices(map_f);
    mapt.next("Map all edges noop time");
  }

  auto retrieve_edges() {
    size_t n = num_vertices();
    size_t m = num_edges();
    using et = tuple<uintV, uintV>;
    auto offsets = pbbs::sequence<uintE>(n);
    parallel_for(0, n, [&] (size_t i) { offsets[i] = 0; });
    auto map_f = [&] (const vertex_entry::entry_t& E, size_t ind) {
      const uintV& v = E.first;
      const auto& EL = E.second; // incs ref-ct
      offsets[v] = EL.degree();
    };
    map_vertices(map_f);
    size_t tot = pbbs::scan_inplace(offsets.slice(), pbbs::addm<uintE>());
    cout << "tot = " << tot << endl;

    auto edges = pbbs::sequence<et>(m);
    auto write_edges = [&] (const vertex_entry::entry_t& E, size_t ind) {
      const uintV& v = E.first;
      const auto& EL = E.second; // incs ref-ct
      size_t off = offsets[v];
//      size_t ct = 0;
      auto map_edges_f = [&] (const uintV& ngh, size_t i) {
        edges[off + i] = make_tuple(v, ngh);
//        return false;
      };
//      EL.iter_elms_cond(v, map_edges_f);

      EL.map_elms(v, map_edges_f);
    };
    map_vertices(write_edges);

    return std::move(edges);
  }

  void test_intersect(uintV a, uintV b) {
    auto m_v1 = find_vertex(a); auto v1 = m_v1.value;
    auto m_v2 = find_vertex(b); auto v2 = m_v2.value;
    size_t int_size = intersect(v1, a, v2, b);
    cout << "int_size = " << int_size << endl;
  }

  void test_union(uintV a, uintV b) {
//    auto v1_src = a; auto v2_src = b;
//    auto m_v1 = find_vertex(v1_src); auto v1 = m_v1.value;
//    auto m_v2 = find_vertex(v2_src); auto v2 = m_v2.value;
//
//    if (!(m_v1.valid && m_v2.valid))
//      return;
//
//    assert(v1.deg > 0); assert(v2.deg > 0);
//    cout << "union: (" << a << ", deg=" << v1.deg << ") with (" << b << ", deg=" << v2.deg << ") " << endl;
//
//    uintV* v1_edges = v1.get_edges(v1_src);
//    uintV* v2_edges = v2.get_edges(v2_src);
//    uintV* tu_and_deg = lists::union_arrs(v1_edges, v1.deg, v2_edges, v2.deg, v1_src);
//    uintV* tu = tu_and_deg+1;
//    size_t ver_deg = tu_and_deg[0];
//
//    timer tt; tt.start();
//    auto uv = uniont(v1, v2, v1_src, false, true);
//    tt.next("union time");
//    if (uv.has_tree()) {
//      assert(uv.root->ref_cnt == 1);
//    }
//
//    uintV* tu_test = uv.get_edges(v1_src);
//    uintV deg_test = uv.deg;
//
//    cout << "Deg should be: " << ver_deg << " deg actually is: " << deg_test << endl;
//    assert(ver_deg == deg_test);
//    for (size_t i=0; i<ver_deg; i++) {
//      assert(tu_test[i] == tu[i]);
//    }
//    cout << "Passed!" << endl;
////    free(v1_edges);
//    free(v2_edges); free(tu_and_deg);
//
//    v2_edges = v2.get_edges(v2_src);
//    tu_and_deg = lists::union_arrs(v1_edges, v1.deg, v2_edges, v2.deg, v1_src);
//    tu = tu_and_deg+1;
//    ver_deg = tu_and_deg[0];
//    cout << "Deg should be: " << ver_deg << " deg actually is: " << deg_test << endl;
//    assert(ver_deg == deg_test);
//    for (size_t i=0; i<ver_deg; i++) {
//      assert(tu_test[i] == tu[i]);
//    }
//    free(v1_edges);
//    free(v2_edges); free(tu_and_deg);
  }

  void test_unions(size_t n_trials) {
    auto r = pbbs::random();
    for (size_t i=0; i<n_trials; i++) {
      size_t a = r.ith_rand(0);
      size_t b = r.ith_rand(1);
      r = r.next();

      test_union(a, b);
    }
  }


  void sort_updates(tuple<uintV, uintV>* edges, size_t m, size_t nn=std::numeric_limits<size_t>::max()) const {
    using edge = tuple<uintV, uintV>;
    auto E_orig = pbbs::make_range(edges, edges + m);
    size_t vtx_bits = pbbs::log2_up(nn);
    if (nn == std::numeric_limits<size_t>::max()) {
      auto max_edge_id = pbbs::delayed_seq<size_t>(m, [&] (size_t i) { return std::max(std::get<0>(E_orig[i]),
            std::get<1>(E_orig[i])); });
      vtx_bits = pbbs::log2_up(pbbs::reduce(max_edge_id, pbbs::maxm<size_t>()));
      nn = 1 << vtx_bits;
    }

    auto edge_to_long = [nn, vtx_bits] (edge e) -> size_t {
      return (static_cast<size_t>(std::get<0>(e)) << vtx_bits) + static_cast<size_t>(std::get<1>(e));
    };
    auto edge_ids_log = pbbs::delayed_seq<size_t>(m, [&] (size_t i) { return pbbs::log2_up(edge_to_long(E_orig[i])); });

    size_t bits = 2*vtx_bits;

    // Only apply integer sort if it will be work-efficient
    if (nn <= (m * pbbs::log2_up(m))) {
      cout << "running integer sort: " << nn << " and mm = " << (m * pbbs::log2_up(m)) << endl;
      pbbs::integer_sort_inplace(E_orig, edge_to_long, bits);
    } else {
      cout << "running sample sort" << endl;
      pbbs::sample_sort_inplace(E_orig, std::less<edge>());
    }
  }

  // m : number of edges
  // edges: pairs of edges to insert. Currently working with undirected graphs;
  // assuming that an edge already shows up in both directions
  // Let the caller delete edges.
  auto insert_edges_batch(size_t m, tuple<uintV, uintV>* edges, bool sorted=false, bool remove_dups=false, size_t nn=std::numeric_limits<size_t>::max(), bool run_seq=false) const {
    // sort edges by their endpoint
    using edge = tuple<uintV, uintV>;
    auto E_orig = pbbs::make_range(edges, edges + m);
    edge* E_alloc = nullptr;
    auto fl = run_seq ? pbbs::fl_sequential : pbbs::no_flag;
    if (!sorted) {
      cout << "insert, nn = " << nn << endl;
      sort_updates(edges, m, nn);
    }

    if (remove_dups) {
      // can perform combining here if desired
      auto bool_seq = pbbs::delayed_seq<bool>(E_orig.size(), [&] (size_t i) {
        return (i == 0 || E_orig[i] != E_orig[i-1]);
      });
      auto E = pbbs::pack(E_orig, bool_seq, fl);
      m = E.size();
      E_alloc = E.to_array();
    }

    auto E = (E_alloc) ? pbbs::make_range(E_alloc, E_alloc + m) : E_orig;

    // pack starts
    auto start_im = pbbs::delayed_seq<size_t>(m, [&] (size_t i) {
      return (i == 0 || (get<0>(E[i]) != get<0>(E[i-1])));
    });
    auto starts = pbbs::pack_index<size_t>(start_im, fl);
    size_t num_starts = starts.size();

    // build new vertices for each start
    using KV = pair<uintV, edge_struct>;
    constexpr const size_t stack_size = 20;
    KV kv_stack[stack_size];
    KV* new_verts = kv_stack;
    if (num_starts > stack_size) {
      new_verts = pbbs::new_array<KV>(num_starts);
    }
    parallel_for(0, num_starts, [&] (size_t i) {
      size_t off = starts[i];
      size_t deg = ((i == (num_starts-1)) ? m : starts[i+1]) - off;
      uintV v = get<0>(E[starts[i]]);

      auto S = pbbs::delayed_seq<uintV>(deg,[&] (size_t i) { return get<1>(E[off + i]); });

      new_verts[i] = std::make_pair(v, edge_struct(S, v, fl));
    }, (run_seq) ? std::numeric_limits<long>::max() : 1);

    auto replace = [run_seq] (const uintV& v, const edge_struct& a, const edge_struct& b) {
      auto ret = tree_plus::uniont(a, b, v, run_seq);

      // Should decrement ref-ct, free if only owner
      lists::deallocate(a.plus);
      tree_plus::Tree_GC::decrement_recursive(a.root, run_seq);

      // Decrement count on b (we don't use copy constructor currently)
      lists::deallocate(b.plus);
      tree_plus::Tree_GC::decrement_recursive(b.root, run_seq);

      return ret;
    };

    // Note that replace is only called if the element currently has a value.
    auto V_next = vertices_tree::multi_insert_sorted_with_values(V.root, new_verts, num_starts, replace, true, run_seq);

    if (num_starts > stack_size) pbbs::free_array(new_verts);
    if (E_alloc) pbbs::free_array(E_alloc);
    return sym_immutable_graph_tree_plus(std::move(V_next));
  }


  // Can also implement a simpler API for removing entire vertices.
  auto delete_edges_batch(size_t m, tuple<uintV, uintV>* edges, bool sorted=false, bool remove_dups=false, size_t nn=std::numeric_limits<size_t>::max(), bool run_seq=false) const {
    // sort edges by their endpoint
    using edge = tuple<uintV, uintV>;
    auto E_orig = pbbs::make_range(edges, edges + m);
    edge* E_alloc = nullptr;
    auto fl = run_seq ? pbbs::fl_sequential : pbbs::no_flag;

    if (!sorted) {
      cout << "delete, nn = " << nn << endl;
      sort_updates(edges, m, nn);
    }

    if (remove_dups) {
      auto bool_seq = pbbs::delayed_seq<bool>(E_orig.size(), [&] (size_t i) {
        return (i == 0 || E_orig[i] != E_orig[i-1]);
      });
      auto E = pbbs::pack(E_orig, bool_seq, fl);
      m = E.size();
      E_alloc = E.to_array();
    }

    auto E = (E_alloc) ? pbbs::make_range(E_alloc, E_alloc + m) : E_orig;

    // pack starts
    auto start_im = pbbs::delayed_seq<size_t>(m, [&] (size_t i) {
      return (i == 0 || (get<0>(E[i]) != get<0>(E[i-1])));
    });
    auto starts = pbbs::pack_index<size_t>(start_im, fl);
    size_t num_starts = starts.size();

    // build new vertices for each start
    using KV = pair<uintV, edge_struct>;
    constexpr const size_t stack_size = 20;
    KV kv_stack[stack_size];
    KV* new_verts = kv_stack;
    if (num_starts > stack_size) {
      new_verts = pbbs::new_array<KV>(num_starts);
    }
    parallel_for(0, num_starts, [&] (size_t i) {
      size_t off = starts[i];
      size_t deg = ((i == (num_starts-1)) ? m : starts[i+1]) - off;
      uintV v = get<0>(E[starts[i]]);

      auto S = pbbs::delayed_seq<uintV>(deg, [&] (size_t i) { return get<1>(E[off + i]); });

      new_verts[i] = make_pair(v, edge_struct(S, v, fl));
    }, run_seq ? std::numeric_limits<long>::max() : 1); // TODO: granularity
    // note that we can filter out unnecessary updates.

    auto replace = [run_seq] (const intV& v, const edge_struct& a, const edge_struct& b) {
      auto ret = tree_plus::difference(b, a, v, run_seq);
      lists::deallocate(a.plus);
      tree_plus::Tree_GC::decrement_recursive(a.root, run_seq);

      lists::deallocate(b.plus);
      tree_plus::Tree_GC::decrement_recursive(b.root, run_seq);

      return ret;
    };
    auto V_next = vertices_tree::multi_insert_sorted_with_values(V.root, new_verts, num_starts, replace, true, run_seq); // TODO: run_seq

    if (num_starts > stack_size) pbbs::free_array(new_verts);
    if (E_alloc) pbbs::free_array(E_alloc);
    return sym_immutable_graph_tree_plus(std::move(V_next));
  }



  // frees offsets and edges
  // TODO(laxmand): clarify that this doesn't do union'ing and is only for
  // building the initial graph.
  auto insert_edges_batch(size_t vn, size_t vm, uintE* offsets, uintV* edges, size_t vertex_off=0) const {
    using KV = pair<uintV, edge_struct>;
    auto new_verts = pbbs::sequence<KV>(vn);
    cout << "building" << endl;
    parallel_for(0, vn, [&] (size_t i) { // TODO: granularity
      uintV v = vertex_off + i;
      size_t off = offsets[i];
      size_t deg = ((i == (vn-1)) ? vm : offsets[i+1]) - off;
      auto f = [&] (size_t i) { return edges[off + i]; };
      auto S = pbbs::sequence<uintV>(deg, f);
      long last_id = -1;
      for (size_t j=0; j<deg; j++) {
        uintV id = S[j];
        if (id <= last_id) {
          assert(false);
        }
        last_id = S[j];
      }

      if (deg > 0) {
        auto m_vert = V.find(v);
        assert(!m_vert.valid);
        new_verts[i] = make_pair(v, edge_struct(S, v));
      } else {
        new_verts[i] = make_pair(v, edge_struct());
      }
//      new_verts[i].second.check_consistency(v);
    }, 1);
    cout << "built" << endl;

    auto replace = [] (const edge_struct& a, const edge_struct& b) {
      assert(false);
      return b;};
    auto V_next = vertices_tree::multi_insert_sorted(V.root, new_verts.to_array(), new_verts.size(), replace, true);
    pbbs::free_array(offsets); pbbs::free_array(edges);
    cout << "inserted" << endl;
    return sym_immutable_graph_tree_plus(std::move(V_next));
  }

  static void init(size_t n, size_t m) {
    // edge_lists are the 'tree nodes' in edgelists
    using edge_list = tree_plus::edge_list;
    edge_list::init(); edge_list::reserve(n/16);

    lists::init(n);

    vertices::init(); vertices::reserve(n/300);
  }

  // frees offsets and edges.
  sym_immutable_graph_tree_plus(size_t n, size_t m, uintE* offsets, uintV* edges) {
    init(n, m);

    print_stats();
    cout << "stats before build" << endl << endl;

    timer build_t; build_t.start();
    using KV = pair<uintV, edge_struct>;
    auto new_verts = pbbs::sequence<KV>(n);
    parallel_for(0, n, [&] (size_t i) { // TODO: granularity
      size_t off = offsets[i];
      size_t deg = ((i == (n-1)) ? m : offsets[i+1]) - off;
      auto S = pbbs::delayed_seq<uintV>(deg, [&] (size_t j) { return edges[off + j]; });

      if (deg > 0) {
        new_verts[i] = make_pair(i, edge_struct(S, i));
      } else {
        new_verts[i] = make_pair(i, edge_struct());
      }
//      new_verts[i].second.check_consistency(i);
    }, 1);
    cout << "built all vertices" << endl;

    print_stats();
    cout << "stats after parallel build" << endl << endl;

    auto replace = [] (const edge_struct& a, const edge_struct& b) {return b;};
    V = vertices_tree::multi_insert_sorted(nullptr, new_verts.begin(), new_verts.size(), replace, true);

    print_stats();
    cout << "stats after multi_insert" << endl << endl;

    pbbs::free_array(offsets); pbbs::free_array(edges);
    new_verts.clear();
    build_t.next("Build time");

//    // Create a list of edge pairs representing the graph.
//    timer build_t; build_t.start();
//    using edge_pair = pair<uintV, uintV>;
//    auto S = pbbs::sequence<edge_pair>(m);
//    cout << "Alloc'd pairs" << endl;
//    parallel_for(size_t i=0; i<n; i++) {
//      size_t off = offsets[i];
//      size_t deg = ((i == n-1) ? m : offsets[i+1]) - off;
//      if (i == 72362876) {
//        cout << "deg 72362876 = " << deg << endl;
//      } else if (i == 4) {
//        cout << "deg 4 = " << deg << endl;
//      }
//      granular_for (j, 0, deg, (deg > 2000), {
//        S[off + j] = make_pair(i, edges[off+j]);
//      });
//    }
//    free(offsets); free(edges);
//    cout << "Wrote pairs, building with multi_insert_reduce now" << endl;
//    cout << "n = " << n << " m = " << m << endl;
//
//    auto reduce = [] (uintV src, pbbs::sequence<uintV> R) {
//      if (src == 4) {
//        cout << "4's deg: " << R.size() << endl;
//      }
//      if (R.size() > 500000000) {
//        cout << "Building a big boy, size = " << R.size() << " src = " << src << endl;
//      }
//      return edge_struct(R, src); };
//    V = vertices::multi_insert_reduce(vertices(), S, reduce, true);
//
//    build_t.next("Build time");
  }

  sym_immutable_graph_tree_plus() {
    V.root=nullptr;
  }

  sym_immutable_graph_tree_plus(vertices _V) : V(std::move(_V)) {}

  static void print_stats() {
    using edge_list = tree_plus::edge_list;

    cout << "Vertices" << endl;
    vertices::print_stats();
    vertices::print_used();
    cout << "Tree list nodes" << endl;
    edge_list::print_stats();
    edge_list::print_used();

    lists::print_stats();
  }


  void print_compression_stats() {
    using edge_list = tree_plus::edge_list;
    size_t n = num_vertices();
    size_t m = num_edges();

    lists::print_stats();
    size_t edge_list_bytes = edge_list::get_used_bytes();
    size_t lists_bytes = lists::get_used_bytes();
    size_t vertex_bytes = vertices::get_used_bytes();
    size_t total_bytes = vertex_bytes + edge_list_bytes + lists_bytes;
    cout << endl << "== Aspen Difference Encoded Stats (DE) ==" << endl;
    cout << "vertex tree: used nodes = " << vertices::used_node() << " size/node = " << vertices::node_size() << " memory used = " << format_gb(vertex_bytes) << " GB" << endl;
    cout << "edge bytes: used nodes = " << edge_list::used_node() << " size/node = " << edge_list::node_size() << " memory used = " << format_gb(edge_list_bytes) << " GB" << endl;
    cout << "compressed-lists memory used = " << format_gb(lists_bytes) << " GB" << endl;
    cout << "Total: " << format_gb(total_bytes) << " GB" << endl;

    cout << endl << "== Aspen No Difference Encoding Stats (No DE) ==" << endl;
    // Each edge uses 4 bytes each
    cout << "vertex tree: used nodes = " << vertices::used_node() << " size/node = " << vertices::node_size() << " memory used = " << format_gb(vertex_bytes) << " GB" << endl;
    cout << "edge bytes: used nodes = " << edge_list::used_node() << " size/node = " << edge_list::node_size() << " memory used = " << format_gb(edge_list_bytes) << " GB" << endl;
    auto sizes = pbbs::sequence<size_t>(n);
    auto map_f = [&] (const vertex_entry::entry_t& E, size_t ind) {
      const uintV& u = E.first;
      const auto& EL = E.second; // incs ref-ct
      sizes[u] = EL.uncompressed_size(u);
    };
    map_vertices(map_f);
    size_t el_size = pbbs::reduce(sizes, pbbs::addm<size_t>());
    cout << "compressed-lists (no difference-encoding) memory used = " << format_gb(el_size) << " GB" << endl;
    cout << "Total: " << format_gb(vertex_bytes + edge_list_bytes + el_size) << " GB" << endl;

    cout << endl << "== Aspen No C-Trees Stats (Uncompressed) ==" << endl;
    vertex_bytes = n*48;
    edge_list_bytes = m*32;
    cout << "vertex tree: used nodes = " << n << " size/node = " << 48 << " memory used = " << format_gb(vertex_bytes) << " GB" << endl;
    cout << "edge tree: used nodes = " << m << " size/node = " << 32 << " memory used = " << format_gb(edge_list_bytes) << " GB" << endl;
    cout << "Total: " << format_gb(vertex_bytes + edge_list_bytes) << " GB" << endl;
  }

  void del() {
    V.~vertices();
    V.root = nullptr;
  }

};
