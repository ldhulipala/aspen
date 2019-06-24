#pragma once
#include "gc.h"
#include "utils.h"

// *******************************************
//   SEQUENCES
// *******************************************

template<class Tree>
struct sequence_ops : Tree {
  using node = typename Tree::node;
  using ET = typename Tree::ET;
  using GC = gc<Tree>;

  static node* join(node* l, ET e, node* r) {
    node *x = Tree::make_node(e);
    return Tree::node_join(l, r, x);
  }

  static node_size_t depth(node* a) {
    if (a == NULL) return 0;
    auto P = utils::fork<node_size_t>(Tree::size(a) >= utils::node_limit,
		[&]() {return depth(a->lc);},
		[&]() {return depth(a->rc);});
    return std::max(P.first, P.second) + 1;
  }

  static bool check_balance(node* a) {
    if (a == NULL) return true;
    auto P = utils::fork<bool>(Tree::size(a) >= utils::node_limit,
		[&]() {return check_balance(a->lc);},
		[&]() {return check_balance(a->rc);});
    return Tree::is_balanced(a) && P.first && P.second;
  }

  static node* select(node* b, size_t rank) {
    size_t lrank = rank;
    while (b) {
      size_t left_size = Tree::size(b->lc);
      if (lrank > left_size) {
	lrank -= left_size + 1;
	b = b->rc;
      }
      else if (lrank < left_size) b = b->lc;
      else return b;
    }
    return NULL;
  }

  static node* join2_i(node* b1, node* b2, bool extra_b1, bool extra_b2) {
    if (!b1) return GC::inc_if(b2, extra_b2);
    if (!b2) return GC::inc_if(b1, extra_b1);

    if (Tree::size(b1) > Tree::size(b2)) {
      bool copy_b1 = extra_b1 || b1->ref_cnt > 1;
      node* l = GC::inc_if(b1->lc, copy_b1);
      node* r = join2_i(b1->rc, b2, copy_b1, extra_b2);
      return Tree::node_join(l, r, GC::copy_if(b1, copy_b1, extra_b1));
    }
    else {
      bool copy_b2 = extra_b2 || b2->ref_cnt > 1;
      node* l = join2_i(b1, b2->lc, extra_b1, copy_b2);
      node* r = GC::inc_if(b2->rc, copy_b2);
      return Tree::node_join(l, r, GC::copy_if(b2, copy_b2, extra_b2));
    }
  }

  static node* join2(node* b1, node* b2) {
    return join2_i(b1, b2, false, false);
  }

  template<class InTree, class Func>
  static node* map(typename InTree::node* b, const Func& f) {
    if (!b) return NULL;
    size_t n = InTree::size(b);
    auto P = utils::fork_app<node*>(n >= utils::node_limit,
       [&] () {return map<InTree>(b->lc, f);},
       [&] () {return map<InTree>(b->rc, f);});
    auto y = f(InTree::get_entry(b));
    auto r = join(P.first, y , P.second);
    return r;
  }

  template<typename F>
  static void foreach_index(node* a, size_t start, const F& f,
			    size_t granularity=utils::node_limit,
			    bool extra_ptr = false) {
    if (!a) return;
    bool copy = extra_ptr || (a->ref_cnt > 1);
    size_t lsize = Tree::size(a->lc);
    f(Tree::get_entry(a), start+lsize);
    utils::fork_app_no_result(lsize >= granularity,
      [&] () {foreach_index(a->lc, start, f, granularity, copy);},
      [&] () {foreach_index(a->rc, start + lsize + 1, f, granularity, copy);});
    if (!extra_ptr) { GC::decrement(a); }
  }

  // similar to above but sequential using in-order traversal
  // usefull if using 20th century constructs such as iterators
  template<typename F>
  static void foreach_seq(node* a, const F& f) {
    if (!a) return;
    foreach_seq(a->lc, f);
    f(Tree::get_entry(a));
    foreach_seq(a->rc, f);
  }

  template<typename F>
  static bool iter_elms_cond(node* a, const F& f) {
    if (!a) return false;
    if (!iter_elms_cond(a->lc, f) && !f(Tree::get_entry(a))) {
      return iter_elms_cond(a->rc, f);
    }
    return true;
  }

  template<typename F>
  static void iter_elms(node* a, const F& f) {
    if (!a) return;
    iter_elms(a->lc, f);
    f(Tree::get_entry(a));
    iter_elms(a->rc, f);
  }

  template<typename F>
  static bool iter_nodes_cond(node* a, const F& f) {
    if (!a) return false;
    if (!iter_nodes_cond(a->lc, f) && !f(Tree::get_entry(a), a)) {
      return iter_nodes_cond(a->rc, f);
    }
    return true;
  }

  // TODO: cleanup this messy code I wrote
  template<typename F>
  static void foreach_entry_seq(node* a, const F& f) {
    if (!a) return;
    f(Tree::get_entry(a));
    foreach_entry_seq(a->lc, f);
    foreach_entry_seq(a->rc, f);
  }

  template<typename F>
  static void foreach_entry(node* a, const F& f,
			    size_t granularity=utils::node_limit) {
    if (!a) return;
    size_t lsize = Tree::size(a->lc);
    if (lsize > granularity) {
      f(Tree::get_entry(a));
      utils::fork_app_no_result(true,
        [&] () {foreach_entry(a->lc, f, granularity);},
        [&] () {foreach_entry(a->rc, f, granularity);});
    } else {
      foreach_entry_seq(a, f);
    }
  }


  template<typename F>
  static void foreach_index_bc_seq(node* a, size_t start, const F& f) {
    if (!a) return;
    size_t lsize = Tree::size(a->lc);
    f(Tree::get_entry(a), start+lsize);
    foreach_index_bc_seq(a->lc, start, f);
    foreach_index_bc_seq(a->rc, start + lsize + 1, f);
  }

  template<typename F>
  static void foreach_index_bc(node* a, size_t start, const F& f,
			    size_t granularity=utils::node_limit) {
    if (!a) return;
    size_t lsize = Tree::size(a->lc);
    if (lsize > granularity) {
      f(Tree::get_entry(a), start+lsize);
      utils::fork_app_no_result(true,
        [&] () {foreach_index_bc(a->lc, start, f, granularity);},
        [&] () {foreach_index_bc(a->rc, start + lsize + 1, f, granularity);});
    } else {
      foreach_index_bc_seq(a, start, f);
    }
  }

  // special cases size
  // g must check nullptr; returns augmented size
  // h: returns node size. no need to check nullptr
  template<typename F, typename G, typename H>
  static void foreach_ind_entry_bc(node* a, size_t start, const F& f, const G& g, H& h) {
    if (!a) return;
    size_t lsize = g(a->lc);
    f(Tree::get_entry(a), start+lsize);
    foreach_ind_entry_bc(a->lc, start, f, g, h);
    size_t a_size = lsize + h(a);
    foreach_ind_entry_bc(a->rc, start + a_size, f, g, h);
  }

  template<typename F, typename G, typename H>
  static void foreach_ind_entry(node* a, size_t start, const F& f, G& g, H& h,
			    size_t granularity=utils::node_limit) {
    if (!a) return;
    size_t n_tree_nodes_left = Tree::size(a->lc);
    if (n_tree_nodes_left > granularity) {
      size_t l_size = g(a->lc);
      f(Tree::get_entry(a), start+l_size);
      size_t a_size = l_size + h(a);
      utils::fork_app_no_result(true,
        [&] () {foreach_ind_entry(a->lc, start, f, g, h, granularity);},
        [&] () {foreach_ind_entry(a->rc, start + a_size, f, g, h, granularity);});
    } else {
      foreach_ind_entry_bc(a, start, f, g, h);
    }
  }

  template<class Func>
  static node* filter(node* b, const Func& f, bool extra_ptr = false) {
    if (!b) return NULL;
    bool copy = extra_ptr || (b->ref_cnt > 1);

    auto P = utils::fork_app<node*>(Tree::size(b) >= utils::node_limit,
      [&]() {return filter(b->lc, f, copy);},
      [&]() {return filter(b->rc, f, copy);});

    if (f(Tree::get_entry(b))) {
      return Tree::node_join(P.first, P.second, GC::copy_if(b, copy, extra_ptr));
    } else {
      GC::dec_if(b, copy, extra_ptr);
      return join2(P.first, P.second);
    }
  }

  template<class Func>
  static bool if_exist(node* b, const Func& f, bool* indicator) {
    if (!b) return false;
	if (*indicator) return true;
	if (f(Tree::get_entry(b))) {
		*indicator = true;
		return true;
	}

    auto P = utils::fork_app<bool>(Tree::size(b) >= utils::node_limit,
      [&]() {return if_exist(b->lc, f, indicator);},
      [&]() {return if_exist(b->rc, f, indicator);});
	return P.first || P.second;
  }

  // Assumes the input is sorted and there are no duplicate keys
  static node* from_array(ET* A, size_t n) {
    if (n <= 0) return Tree::empty();
    if (n == 1) return Tree::single(A[0]);
    size_t mid = n/2;

    node* m = Tree::make_node(A[mid]);

    auto P = utils::fork_app<node*>(n >= utils::node_limit,
      [&]() {return from_array(A, mid);},
      [&]() {return from_array(A+mid+1, n-mid-1);});

    return Tree::node_join(P.first, P.second, m);
  }

  template<class Seq1, class Func>
  static node* map_filter(typename Seq1::node* b, const Func& f) {
    if (!b) return NULL;

    auto P = utils::fork_app<node*>(Seq1::size(b) >= utils::node_limit,
      [&]() {return map_filter<Seq1>(b->lc, f);},
      [&]() {return map_filter<Seq1>(b->rc, f);});

    maybe<ET> me = f(Seq1::get_entry(b));
    if (me) {
      node* r = Tree::make_node(*me);
      return Tree::node_join(P.first, P.second, r);
    } else return join2(P.first, P.second);
  }

  template<class Reduce>
  static typename Reduce::t map_reduce_node(node* b, Reduce r,
				       size_t grain=utils::node_limit) {
    using t = typename Reduce::t;
    if (!b) return r.identity();

    auto P = utils::fork_app<t>(Tree::size(b) >= grain,
      [&]() {return map_reduce_node(b->lc, r, grain);},
      [&]() {return map_reduce_node(b->rc, r, grain);});

    //t v = r.from_entry(*Tree::get_entry_p(b));
    t v = r.from_node(b);
    return r.combine(P.first, r.combine(v, P.second));
  }

  template<class Reduce>
  static typename Reduce::t map_reduce(node* b, Reduce r,
				       size_t grain=utils::node_limit) {
    using t = typename Reduce::t;
    if (!b) return r.identity();

    auto P = utils::fork_app<t>(Tree::size(b) >= grain,
      [&]() {return map_reduce(b->lc, r, grain);},
      [&]() {return map_reduce(b->rc, r, grain);});

    //t v = r.from_entry(*Tree::get_entry_p(b));
    t v = r.from_entry(Tree::get_entry(b));
    return r.combine(P.first, r.combine(v, P.second));
  }

  template<class T, class Map, class Reduce>
  static T map_reducef(node* b, Map m, Reduce r, T identity,
		       size_t grain=utils::node_limit) {
    if (!b) return identity;

    auto P = utils::fork_app<T>(Tree::size(b) >= grain,
      [&] () {return map_reducef(b->lc, m, r, identity, grain);},
      [&] () {return map_reducef(b->rc, m, r, identity, grain);});

    T v = m(Tree::get_entry(b));
    return r(P.first, r(v, P.second));
  }
};
