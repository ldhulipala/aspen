#pragma once

#include "../pbbslib/random_shuffle.h"
#include "../lib_extensions/sparse_table.h"

template <class Graph, class VS, class Vtxs, class P>
auto get_nghs(Graph& G, VS& vs, Vtxs& vtxs, P p) {
  size_t n = G.num_vertices();
  timer ct; ct.start();
  vs.to_sparse();
  ct.stop();
  auto deg_im = pbbs::delayed_seq<size_t>(vs.size(), [&] (size_t i) { return vtxs[vs.s[i]].degree();});
  size_t sum_d = pbbs::reduce(deg_im, pbbs::addm<size_t>());
  // cout << "sum_d = " << sum_d << " G.m/100 = " << (G.num_edges()/100) << endl;

  if (sum_d > G.num_edges() / 100) { // dense forward case
    timer dt; dt.start();
    auto dense = pbbs::sequence<bool>(n, false);
    auto map_f = [&] (const uintV& src, const uintV& ngh) {
      if (p(ngh) && !dense[ngh]) dense[ngh] = 1;
    };
    parallel_for(0, vs.size(), [&] (size_t i) {
      uintV v = vs.s[i];
      vtxs[v].map_nghs(v, map_f);
    }, 1);
//    dt.next("dense time");
    return vertex_subset(n, dense.to_array());
  } else { // sparse case
    // iterate, and add vertices to HT.
    timer st; st.start();
    auto ht = sparse_table<uintV, pbbs::empty>(sum_d, make_tuple(UINT_E_MAX, pbbs::empty()));
    parallel_for(0, vs.size(), [&] (size_t i) {
      auto map_f = [&] (const uintV& src, const uintV& ngh) {
        if (p(ngh)) ht.insert(make_tuple(ngh, pbbs::empty()));
      };
      uintV v = vs.s[i];
      vtxs[v].map_nghs(v, map_f);
    }, 1);
    auto nghs = ht.entries();
    ht.del();
//    st.next("sparse time");
    auto nghs_size = nghs.size();
    return vertex_subset(n, nghs_size, (uintV*)nghs.to_array());
  }
}

struct mis_f {
  intV* p;
  uintV* perm;
  mis_f(intV* _p, uintV* _perm) : p(_p), perm(_perm) { }
  inline bool update (const uintV& s, const uintV& d) {
    if (perm[s] < perm[d]) {
      p[d]--;
      return p[d] == 0;
    }
    return false;
  }
  inline bool updateAtomic (const uintV& s, const uintV& d) {
    if (perm[s] < perm[d]) {
      return (pbbs::fetch_and_add(&p[d], -1) == 1);
    }
    return false;
  }
  inline bool cond (uintV d) { return (p[d] > 0); } // still live
};

template <class Graph>
auto MIS(Graph& G, bool print_stats=false) {
  timer init_t; init_t.start();
  size_t n = G.num_vertices();
  auto vtxs = G.fetch_all_vertices();

  // 1. Compute the priority DAG
  auto priorities = pbbs::sequence<intV>(n);

  auto perm = pbbs::random_permutation<uintV>(n);
  parallel_for(0, n, [&] (size_t i) {
    uintV our_pri = perm[i];
    auto count_f = [&] (const uintV& src, const uintV& ngh) {
      uintV ngh_pri = perm[ngh];
      return ngh_pri < our_pri;
//      if (ngh_pri < our_pri) {
//        ct++;
//      }
      return false;
    };
//    vtxs[i].iter_elms_cond(i, count_f);
//    priorities[i] = ct;
    priorities[i] = vtxs[i].count_nghs(i, count_f);
  }, 1);
//  auto pri_im = pbbs::delayed_seq<size_t>(n, [&] (size_t i) { return priorities[i]; });
//  cout << "sum pri = " << pbbs::reduce(pri_im) << endl;

  // 2. Compute the initial rootset
  auto zero_map = pbbs::delayed_seq<bool>(n, [&] (size_t i) { return priorities[i] == 0; });
  auto init = pbbs::pack_index<uintV>(zero_map);
  auto init_size = init.size();
  auto roots = vertex_subset(n, init_size, init.to_array());

  auto in_mis = pbbs::sequence<bool>(n, false);
  size_t finished = 0;
  size_t rounds = 0;
//  init_t.next("init");
  timer sparse_t, dense_t, other_t;
  while (finished != n) {
    assert(roots.size() > 0);
    cout << "round = " << rounds << " size = " << roots.size()
         << " remaining = " << (n - finished) << endl;

    // 3. Set the roots in the MIS
    vertex_map(roots, [&] (uintV v) {
      in_mis[v] = true; // assert(priorities[v] == 0);
    });

    // 4. Compute neighbors of roots that are still live
    auto removed = get_nghs(G, roots, vtxs, [&] (const uintV& ngh) { return priorities[ngh] > 0; });
    vertex_map(removed, [&] (uintV v) {
      priorities[v] = 0;
    });

    // 5. Compute the new roots: these are neighbors of removed that have
    // their priorities set to 0 after eliminating all nodes in removed
    intV* pri = priorities.begin();
    timer nr; nr.start();
    auto new_roots = G.edge_map(removed, mis_f(pri, perm.begin()), vtxs, sparse_t, dense_t, other_t);
//    nr.next("new roots time");

    // 6. Update finished with roots and removed. Update roots.
    finished += roots.size();
    finished += removed.size();
    removed.del(); roots.del();

    roots = new_roots;
    rounds++;
  }
  return std::move(in_mis);
}
 // MIS_rootset
