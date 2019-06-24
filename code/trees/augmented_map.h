#pragma once

using namespace std;

// *******************************************
//   AUG MAPS
// *******************************************

template <class _Entry, class Join_Tree>
struct aug_map_ : private map_<_Entry, Join_Tree> {
public:
  using Map = map_<_Entry, Join_Tree>;
  using Entry = typename Map::Entry;
  using Tree = augmented_ops<typename Map::Tree>;
  using node = typename Tree::node;
  using E = typename Entry::entry_t;
  using K = typename Entry::key_t;
  using V = typename Entry::val_t;
  using A = typename Entry::aug_t;
  using M = aug_map_;
  using GC = typename Map::GC;
  using maybe_V = maybe<V>;
  using maybe_E = maybe<E>;

  template<class F>
  static M aug_filter(M m, const F& f) {
    return M(Tree::aug_filter(m.get_root(), f)); }

  // extract the augmented values
  A aug_val() const { return Tree::aug_val(Map::root); }

  A aug_left (const K& key) {
    typename Tree::aug_sum_t a;
    Tree::aug_sum_left(Map::root, key, a);
    return a.result;}

  A aug_right(const K& key) {
    typename Tree::aug_sum_t a;
    Tree::aug_sum_right(Map::root, key, a);
    return a.result;}

  A aug_range(const K& key_left, const K& key_right) {
    typename Tree::aug_sum_t a;
    Tree::aug_sum_range(Map::root, key_left, key_right, a);
    return a.result;}

  // just side effecting
  template <class restricted_sum>
  void range_sum(const K& key_left,
		 const K& key_right,
		 restricted_sum& rs) {
    Tree::aug_sum_range(Map::root, key_left, key_right, rs);
  }

  template <class Func>
  maybe_E aug_select(Func f) {
    return Map::node_to_entry(Tree::aug_select(Map::root, f));};

  static M insert_lazy(M m, const E& p) {
    auto replace = [] (const V& a, const V& b) {return b;};
    return M(Tree::insert_lazy(m.get_root(), p, replace)); }

  // for coercing a map to an aug_map
  aug_map_(Map&& m) {if (this != &m) {this->root = m.root; m.root = NULL;} }
  aug_map_() : Map() { }
  // install Map's constructors
  using Map::Map;

  template <class Func>
  static M insert(M m, const E& p, const Func& f) {
    return Map::insert(std::move(m),p,f);}
  static M insert(M m, const E& p) {//cout << "ins a " << endl;
    return Map::insert(std::move(m),p);}
  static M remove(M m, const K& k) {return Map::remove(std::move(m), k);}
  template<class F>
  static M filter(M m, const F& f) {return Map::filter(std::move(m), f);}
  static M multi_insert(M m, pbbs::sequence<E> const &SS, bool seq_inplace = false) {
    return Map::multi_insert(std::move(m), SS, seq_inplace);}
  template<class Bin_Op>
  static M multi_insert_combine(M m, pbbs::sequence<E> const &S, Bin_Op f,
				bool seq_inplace = false) {
    return Map::multi_insert_combine(std::move(m), S, f, seq_inplace);}
  template<class Val, class Reduce>
  static M multi_insert_reduce(M m, pbbs::sequence<pair<K,Val>> const &S, Reduce g, bool sorted=false) {
    return Map::multi_insert_reduce(std::move(m), S, g, sorted); }
  static M multi_insert_sorted(M m, pbbs::sequence<E> const &S, bool run_seq=false) {
    return Map::multi_insert_sorted(std::move(m), S, false, run_seq); }
  template<class M1, class M2, class F>
  static M map_intersect(M1 a, M2 b, const F& op) {
    return Map::map_intersect(std::move(a), std::move(b), op);}
  static M map_intersect(M a, M b) {return Map::map_intersect(a, b);}
  template<class F>
  static M map_union(M a, M b, const F& op) {return Map::map_union(std::move(a), std::move(b), op);}
  static M map_union(M a, M b) {return Map::map_union(std::move(a), std::move(b));}
  static M map_difference(M a, M b) {return Map::map_difference(std::move(a), std::move(b));}
  static M join2(M a, M b) {return Map::join2(std::move(a), std::move(b));}
  static M range(M& a, K kl, K kr) {return Map::range(a,kl,kr);}
  static M upTo(M& a, K kr) {return Map::upTo(a,kr);}
  template<class Ma, class F>
  static M map(Ma a, const F f) {return Map::map(a, f);}
  static void entries(M m, E* out) { Map::entries(std::move(m),out);}
  template <class outItter>
  static void keys(M m, outItter out) {Map::keys(std::move(m),out);}
  bool operator == (const M& m) { return Map::operator==(m);}
//  template<class T, class Map, class Reduce>
//  static T map_reducef(M a, Map m, Reduce r, T identity) {
//    return Map::map_reducef(a, m, r, identity);}
  template<class Reduce>
  static typename Reduce::t map_reduce(M a, Reduce r) {
    return Map::map_reduce(a,r);}
  template<class Reduce>
  static typename Reduce::t map_reduce_node(M a, Reduce r) {
    return Map::map_reduce_node(a,r);}
  template<class F>
  static void map_index(M m, const F& f) {
	Map::map_index(m,f); }
  template<class Ma, class F>
  static M map_filter(Ma a, const F& f) {return Map::map_filter(a,f);}

public:
  using Map::map_reduce;
  using Map::map_reducef;
  using Map::size;
  using Map::is_empty;
  using Map::init;
  using Map::reserve;
  using Map::finish;
  using Map::clear;
  using Map::find;
  using Map::map_elms;
  using Map::map_elms_no_index;
  using Map::map_elms_entry;
  using Map::iter_elms_cond;
  using Map::iter_elms;
  using Map::iter_nodes_cond;
  using Map::contains;
  using Map::next;
  using Map::previous;
  using Map::rank;
  using Map::select;
  using Map::root;
  using Map::get_root;
  using Map::insert;
  using Map::print_stats;
  using Map::print_used;
  using Map::get_used_bytes;
  using Map::used_node;
  using Map::node_size;
};

// creates a key-value pair for the entry, and redefines from_entry
template <class entry>
struct aug_map_full_entry : entry {
  using val_t = typename entry::val_t;
  using key_t = typename entry::key_t;
  using aug_t = typename entry::aug_t;
  using entry_t = std::pair<key_t,val_t>;
  static inline key_t get_key(const entry_t& e) {return e.first;}
  static inline val_t get_val(const entry_t& e) {return e.second;}
  static inline void set_val(entry_t& e, const val_t& v) {e.second = v;}
  static inline aug_t from_entry(const entry_t& e) {
    return entry::from_entry(e.first, e.second);}
};

template <class _Entry, class Balance=weight_balanced_tree>
using aug_map =
  aug_map_<aug_map_full_entry<_Entry>,
  typename Balance::template
  balance<aug_node<typename Balance::data,aug_map_full_entry<_Entry>>>>;
