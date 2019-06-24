#pragma once

using namespace std;

// *******************************************
//   MAPS
// *******************************************

template <class _Entry, class Join_Tree>
class map_ {
public:
  using Entry = _Entry;
  using Seq_Tree = sequence_ops<Join_Tree>;
  using Tree = map_ops<Seq_Tree,Entry>;
  using node = typename Tree::node;
  using E = typename Entry::entry_t;
  using K = typename Entry::key_t;
  using V = typename Entry::val_t;
  using M = map_;
  using GC = typename Tree::GC;
  using Build = build<Entry>;
  using maybe_V = maybe<V>;
  using maybe_E = maybe<E>;

  // initializing, reserving and finishing
  static void init() { GC::init(); }
  static void reserve(size_t n, bool randomize=false) {
    GC::reserve(n, randomize);};
  static void finish() { GC::finish(); }

  // empty constructor
  map_() : root(NULL) { GC::init(); }

  // copy constructor, increment reference count
  map_(const M& m) {//if (root != NULL) cout << "copy: " << (m.root)->ref_cnt << endl;
    root = m.root; GC::increment(root);}

  // move constructor, clear the source, leave reference count as is
  map_(M&& m) { //cout << "move constructor: " <<endl;
    root = m.root; m.root = NULL;}

  // copy assignment, clear target, increment reference count,
  M& operator = (const M& m) {
    if (this != &m) {
	//node* tmp = root;
	clear();
	root = m.root; GC::increment(root);
	//if (GC::initialized()) GC::decrement_recursive(tmp);
	}
    return *this;
  }

  M& get(const M& m) {
    if (this != &m) {
		clear(); root = m.root; GC::increment(root);
	}
    return *this;
  }

  // move assignment, clear target, leave reference count as is
  M& operator = (M&& m){
    if (this != &m) { clear(); root = m.root; m.root = NULL;}
    return *this;
  }

  // destruct.
  ~map_() { clear(); }

  // singleton
  map_(const E& e) { GC::init(); root = Tree::single(e);}

  // construct from an array keeping one of the equal keys
  map_(E* s, E* e, bool seq_inplace = false) {
    M empty = M();
    root = multi_insert(empty,pbbs::make_range(s,e), seq_inplace).get_root(); }

  // construct from an array keeping one of the equal keys
  template<class Bin_Op>
  map_(E* s, E* e, Bin_Op f, bool seq_inplace = false) {
    M empty = M();
    root = multi_insert_combine(empty,pbbs::make_range(s,e),f,seq_inplace).get_root(); }

  // construct from sequence keeping one of the equal keys
  map_(pbbs::sequence<E> const &S, bool seq_inplace = false) {
    M empty = M();
    root = multi_insert(empty, S, seq_inplace).get_root(); }

  // construct from sequence keeping one of the equal keys
  template<class Seq, class Bin_Op>
  map_(Seq S, Bin_Op f, bool seq_inplace = false) {
    M empty = M();
    root = multi_insert_combine(empty, S, f, seq_inplace).get_root(); }

  // clears contents, decrementing ref counts
  void clear() {
    if (GC::initialized()) {
      GC::decrement_recursive(root);
    }
    root = NULL;
  }

  static void print_stats() {
    GC::print_stats();
  }

  static void print_used() {
    GC::used_bytes();
  }

  static size_t get_used_bytes() {
    return GC::get_used_bytes();
  }

  static size_t used_node() {
    return GC::used_node();
  }

  static size_t node_size() {
    return GC::node_size();
  }

  // some basic functions
  size_t size() const {
    //cout << "size: " << Tree::size(root) << "; " << root->ref_cnt << endl;
    return Tree::size(root); }

  // equality
  bool operator == (const M& m) const {
    return (size() == m.size()) && (size() == map_union(*this,m).size());
  }

  template <class F>
  static void foreach_index(M m, F f) {
    Tree::foreach_index(m.get_root(), 0, f);
  }

  template <class OT, class F>
  static pbbs::sequence<OT> to_seq(M m, F f) {
    pbbs::sequence<OT> out = pbbs::sequence<OT>::alloc_no_init(m.size());
    auto g = [&] (E e, size_t i) {
      pbbs::assign_uninitialized(out[i],f(e));};
    Tree::foreach_index(m.get_root(), 0, g);
    return out;
  }

  static void entries(M m, E* out) {
    auto f = [&] (E e, size_t i) {out[i] = e;};
    Tree::foreach_index(m.get_root(), 0, f);
  }

  static void values(M m, V* out) {
    auto f = [&] (E e, size_t i) {out[i] = Entry::get_val(e);};
    Tree::foreach_index(m.get_root(), 0, f);
  }

  template <class outItter>
  static void keys(M m, outItter out) {
    if (m.size() > 1000) {
      auto f = [&] (const E& e, const size_t i) {out[i] = Entry::get_key(e);};
      Tree::foreach_index(m.get_root(), 0, f);
    } else {
      auto g = [&] (E e) {*out = Entry::get_key(e); ++out;};
      Tree::foreach_seq(m.get_root(), g);
    }
  }

  bool operator != (const M& m) const { return !(*this == m); }

  template <class Func>
  static M insert(M m, const E& p, const Func& f) {
    return M(Tree::insert(m.get_root(), p, f)); }

  template <class Func>
  void insert(const E& p, const Func& f) {
    root = Tree::insert(root, p, f); }

  static M insert(M m, const E& p) {
    auto replace = [] (const V& a, const V& b) {return b;};
    return M(Tree::insert(m.get_root(), p, replace)); }

  void insert(const E& p) {
    auto replace = [] (const V& a, const V& b) {return b;};
    root = Tree::insert(root, p, replace); }

  static M remove(M m, const K& k) {
    return M(Tree::deletet(m.get_root(), k)); }

  static M join2(M a, M b) {
    return M(Tree::join2(a.get_root(), b.get_root()));
  }

  bool is_empty() {return root == NULL;}

  // filters elements that satisfy the predicate when applied to the elements.
  template<class F>
  static M filter(M m, const F& f) {
    return M(Tree::filter(m.get_root(), f)); }

  static M from_sorted(pbbs::sequence<E> const &S) {
    return M(Tree::from_array(S.begin(), S.size()));
  }

  template<class F>
  static bool if_exist(M m, const F& f) {
	bool* indicator = new bool;
	*indicator = false;
    bool ans = Tree::if_exist(m.get_root(), f, indicator);
	delete indicator;
	return ans;
  }

  // insert multiple keys from an array
  //template<class Seq>
  static M multi_insert(M m, pbbs::sequence<E> const &SS, bool seq_inplace = false, bool inplace = false) {
    auto replace = [] (const V& a, const V& b) {return b;};
	timer t,t1;
	t.start();
    pbbs::sequence<E> A = Build::sort_remove_duplicates(SS, seq_inplace, inplace);
	//cout << t.stop() << endl;
	t1.start();
    auto x = M(Tree::multi_insert_sorted(m.get_root(), A.begin(),
				       A.size(), replace));
	//cout << t1.stop() << endl;
	return x;
  }

  static M multi_insert(M m, E* A, size_t n) {
    auto replace = [] (const V& a, const V& b) {return b;};
	timer t,t1;
	t.start();
    pbbs::sequence<E> B = Build::sort_remove_duplicates(A, n);
	//cout << t.stop() << endl;
	t1.start();
    auto x =  M(Tree::multi_insert_sorted(m.get_root(),
    				       B.begin(), B.size(), replace));
	//cout << t1.stop() << endl;
    return x;
  }

  // update multiple entries from an array
  //template<class Seq>
  template<class VE, class Bin_Op>
  static M multi_update_sorted(M m, pbbs::sequence<pair<K, VE>> const &SS, Bin_Op f) {
    return M(Tree::multi_update_sorted(m.get_root(), SS.begin(),
				       SS.size(), f));
  }

  // insert multiple keys from an array
  //template<class Seq>
  static M multi_insert_sorted(M m, pbbs::sequence<E> const &SS, bool seq_inplace = false, bool run_seq=false) {
    auto replace = [] (const V& a, const V& b) {return b;};
    return M(Tree::multi_insert_sorted(m.get_root(), SS.begin(),
				       SS.size(), replace, /*extra ptr*/false, run_seq));
  }

  // insert multiple keys from an array, combine duplicates with f
  // here f must have type: V x V -> V
  // if key in map, then also combined with f
  template<class Bin_Op>
  static M multi_insert_combine(M m, pbbs::sequence<E> const &S, Bin_Op f,
				bool seq_inplace = false) {
    pbbs::sequence<E> A = Build::sort_combine_duplicates(S, f, seq_inplace);

    return M(Tree::multi_insert_sorted(m.get_root(),
				       A.begin(), A.size(), f));
  }

  // insert multiple keys from an array, reduce duplicates with g
  // here g must have type: pbbs::sequence<Val> -> V
  // if key in map, then replaced
  template<class Val, class Reduce>
  static M multi_insert_reduce(M m, pbbs::sequence<pair<K,Val>> const &S, Reduce g, bool sorted=false) {
    auto replace = [] (const V& a, const V& b) {return b;};
	timer t,t1;
	t.start();
  pbbs::sequence<E> A = Build::sort_reduce_duplicates(S, g, sorted);
//	std::cout << "outer reduce: " << t.stop() << std::endl;
	t1.start();
    auto x =  M(Tree::multi_insert_sorted(m.get_root(),
    				       A.begin(), A.size(), replace));
//	std::cout << "time multi_insert: " << t1.stop() << std::endl;
    return x;
  }

  template<class Val, class Reduce>
  static M multi_insert_reduce(M m, pair<K,Val>* A, size_t n, Reduce g, bool sorted=false) {
    auto replace = [] (const V& a, const V& b) {return b;};
	timer t,t1;
	t.start();
    auto B = Build::sort_reduce_duplicates(A, n, g);
	t1.start();
    auto x =  M(Tree::multi_insert_sorted(m.get_root(),
    				       B.first, B.second, replace));
    return x;
  }

  template<class Val, class Reduce, class Bin_Op>
  static M multi_insert_reduce(M m, pair<K,Val>* A, size_t n, Reduce g, Bin_Op& f = [] (const V& a, const V& b) {return b;}, bool sorted=false) {
	timer t,t1;
	t.start();
    auto B = Build::sort_reduce_duplicates(A, n, g);
	t1.start();
    auto x =  M(Tree::multi_insert_sorted(m.get_root(),
    				       B.first, B.second, f));
    return x;
  }

  // basic search routines
  maybe_V find(const K& key) const {
    return node_to_val(Tree::find(root, key));}

  template <class Func>
  maybe_V update(const K& key, const Func& f) const {
    return node_to_val(Tree::update(root, key, f));}

  bool contains(const K& key) const {
    return (Tree::find(root, key) != NULL) ? true : false;}

  maybe_E next(const K& key) const {
    return node_to_entry(Tree::next(root, key));}

  maybe_E previous(const K& key) const {
      return node_to_entry(Tree::previous(root, key));}

  // F: (entry, i) -> ()
  template <class F>
  void map_elms(F f, bool run_seq=false, size_t granularity=utils::node_limit) const {
    if (run_seq) {
      Tree::foreach_index_bc_seq(root, 0, f);
    } else {
      Tree::foreach_index_bc(root, 0, f, granularity);
    }
  }

  // F: (entry) -> ()
  template <class F>
  void map_elms_no_index(F f, bool run_seq=false) const {
    if (run_seq) {
      Tree::foreach_entry_seq(root, f);
    } else {
      Tree::foreach_entry(root, f);
    }
  }

  // F: (entry, i) -> ()
  // G: returns augmented size at the node
  // H: returns array size at the node
  template <class F, class G, class H>
  void map_elms_entry(F f, G g, H h) const {
    Tree::foreach_ind_entry(root, 0, f, g, h);
  }

  template<class T, class Map, class Reduce>
  T map_reducef(const Map& m, const Reduce& r, const T& identity,
		       size_t grain=utils::node_limit) {
    return Tree::map_reducef(root, m, r, identity, grain);
  }

  // F: (entry, i) -> ()
  template <class F>
  bool iter_elms_cond(F f) const {
    return Tree::iter_elms_cond(root, f);
  }

  // F: (entry, i) -> ()
  template <class F>
  void iter_elms(F f) const {
    return Tree::iter_elms(root, f);
  }

  // F: (entry, i) -> ()
  template <class F>
  bool iter_nodes_cond(F f) const {
    return Tree::iter_nodes_cond(root, f);
  }



  // rank and select
  size_t rank(const K& key) { return Tree::rank(root, key);}

  maybe_E select(const size_t rank) const {
    return node_to_entry(Tree::select(root, rank));
  }

  template<class F>
  static M map_union(M a, M b, const F& op, bool extra = false) {
    return M(Tree::uniont(a.get_root(), b.get_root(), op, extra));
  }

  static M map_union(M a, M b, bool extra = false) {
    auto get_right = [] (V a, V b) {return b;};
    //cout << "map_union a,b: " << a.size() << ", " << b.size() << endl;
    auto x = M(Tree::uniont(a.get_root(), b.get_root(), get_right, extra));
    //cout << "map_union r: " << x.size() << endl;
    //cout << "find: " << *(x.find(1)) << endl;
    return x;
  }

  template<class M1, class M2, class F>
  static M map_intersect(M1 a, M2 b, const F& op) {
    using T1 = typename M1::Tree;
    using T2 = typename M2::Tree;
    return M(Tree::template intersect<T1,T2>(a.get_root(),
					     b.get_root(), op));
  }

  static M map_intersect(M a, M b) {
    auto get_right = [] (V a, V b) {return b;};
    return M(Tree::template intersect<Tree,Tree>(a.get_root(),
						 b.get_root(), get_right));
  }

  static M map_difference(M a, M b) {
    return M(Tree::difference(a.get_root(), b.get_root()));
  }

  static M range(M& a, K kl, K kr) {
    return M(Tree::range(a.root, kl, kr));
  }

  static M range_number(M& a, K kl, size_t r) {
    return M(Tree::range_num(a.root, kl, r));
  }

  template<class T, class Map, class Reduce>
  static T range_number_mr(M& a, K kl, size_t r, const Map& mp, const Reduce& rdc, const T& I) {
    auto x = Tree::range_num_mr(a.root, kl, r, mp, rdc, I);
	return x.first;
  }

  static M upTo(M& a, K kr) {
    return M(Tree::left(a.root, kr));
  }

  template<class Ma, class F>
  static M map(Ma a, const F& f) {
    GC::init();
    return M(Tree::template map<typename Ma::Tree>(a.root, f));
  }

  template<class F>
  static void map_index(M m, const F& f, size_t granularity=utils::node_limit) {
    Tree::foreach_index(m.get_root(), 0, f, granularity);
  }

  template<class Reduce>
  //static typename Reduce::t map_reduce(M a, Reduce r,
  static typename Reduce::t map_reduce(const M& a, const Reduce& r,
				       size_t grain=utils::node_limit) {
    GC::init();
    return Tree::map_reduce(a.root, r, grain);
  }

  template<class Reduce>
  //static typename Reduce::t map_reduce(M a, Reduce r,
  static typename Reduce::t map_reduce_node(const M& a, const Reduce& r,
				       size_t grain=utils::node_limit) {
    GC::init();
    return Tree::map_reduce_node(a.root, r, grain);
  }


  template<class Ma, class F>
  static  M map_filter(Ma a, const F& f) {
    return M(Tree::template map_filter<typename Ma::Tree>(a.root, f));
  }

  // grabs root by "moving" it.  Important for reuse
  node* get_root() {node* t = root; root = NULL; return t;};

  node* root;

  // construct from a node (perhaps should be private)
  map_(node* n) : root(n) { GC::init(); }

  maybe_V node_to_val(node* a) const {
    if (a != NULL) return maybe_V(Entry::get_val(Tree::get_entry(a)));
    else return maybe_V();
  }

  maybe_E node_to_entry(node* a) const {
    if (a != NULL) return maybe_E(Tree::get_entry(a));
    else return maybe_E();
  }

};

// creates a key-value pair for the entry
template <class entry>
struct map_full_entry : entry {
  using val_t = typename entry::val_t;
  using key_t = typename entry::key_t;
  using entry_t = std::pair<key_t,val_t>;
  static inline key_t get_key(const entry_t& e) {return e.first;}
  static inline val_t get_val(const entry_t& e) {return e.second;}
  static inline void set_val(entry_t& e, const val_t& v) {e.second = v;}
};

template <class _Entry, class Balance=weight_balanced_tree>
using pam_map =
  map_<map_full_entry<_Entry>,
       typename Balance::template
       balance<basic_node<typename Balance::data,
			  typename map_full_entry<_Entry>::entry_t>>>;

// entry is just the key (no value), for use in sets
template <class entry>
struct set_full_entry : entry {
  using key_t = typename entry::key_t;
  using val_t = bool;  // not used
  using entry_t = key_t;
  static inline key_t get_key(const entry_t& e) {return e;}
  static inline val_t get_val(const entry_t& e) {return 0;}
  static inline void set_val(entry_t& e, const val_t& v) {}
};

template <class _Entry, class Balance=weight_balanced_tree>
using pam_set =
  map_<set_full_entry<_Entry>,
       typename Balance::template
       balance<basic_node<typename Balance::data,
			  typename _Entry::key_t>>>;

// entry is just the key (no value), for use in sets
template <class data>
struct seq_full_entry {
  using key_t = data;
  using val_t = bool;  // not used
  using entry_t = data;
  static bool comp(const key_t& a, const key_t& b) {return true;}
  static inline key_t get_key(const entry_t& e) {return e;}
  static inline val_t get_val(const entry_t& e) {return 0;}
  static inline void set_val(entry_t& e, const val_t& v) {}
};

template <class data, class Balance=weight_balanced_tree>
using pam_seq =
  map_<seq_full_entry<data>,
       typename Balance::template
       balance<basic_node<typename Balance::data, data>>>;
