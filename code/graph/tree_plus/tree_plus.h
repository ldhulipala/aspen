#pragma once

#include "compressed_lists.h"
// #include "uncompressed_lists.h"

#include "../../pbbslib/sequence.h"

#define lists compressed_lists
//#define lists uncompressed_lists
//#define CHECK_CORRECTNESS 1

namespace tree_plus {

  using AT = lists::array_type;

  struct edge_entry {
    using key_t = uintV; // the 'head' edge of this node.
    using val_t = AT*; // the set of edges stored in this node.
    static bool comp(const key_t& a, const key_t& b) { return a < b; }
    using aug_t = uintV; // num. edges in this subtree
    static aug_t get_empty() { return 0; }
    static aug_t from_entry(const key_t& k, const val_t& v) { return 1 + lists::node_size(v); }
    static aug_t combine(const aug_t& a, const aug_t& b) { return a + b; }
    using entry_t = std::pair<key_t,val_t>;
    static entry_t copy_entry(const entry_t& e) {
      // TODO: Instead of copying, bump a ref-ct (note that copy_node and
      // deallocate can implement these semantics internally)
      return make_pair(e.first, lists::copy_node(e.second));
    }
    static void del(entry_t& e) {
      if (e.second) {
        // TODO: Should decrement ref-ct, free if only owner
        lists::deallocate(e.second);
      }
    }
  };
  using edge_list = aug_map<edge_entry>;

  struct treeplus {
    using Tree = typename edge_list::Tree;
    using Node = typename edge_list::node;
    using Entry = typename edge_entry::entry_t; // pair<uintV, node>
    using Tree_GC = Tree::GC;

    AT* plus;
    Node* root;

    // Uses two reads to construct the degree instead of explicitly storing it.
    size_t degree() const {
      size_t deg = 0;
      if (plus) {
        deg += lists::node_size(plus);
      }
      if (root) {
        deg += Tree::aug_val(root);
      }
      return deg;
    }
    size_t size() const { return degree(); }

    bool has_tree() { return root != nullptr; }
    bool has_plus() { return plus != nullptr; }
    bool empty() { return root == nullptr && plus == nullptr; }

    void del() {
        // TODO: Should decrement ref-ct, free if only owner
      lists::deallocate(plus);
      auto T = edge_list(); T.root = root;
    }

    // look up edges
    bool contains(uintV src, uintV ngh) const {
      if (root) {
        auto mp = Tree::previous_or_eq(root, ngh);
        if (mp) {
          const Entry& entry = Tree::get_entry(mp);
          if (entry.first == ngh) return true;
          return lists::contains(entry.second, src, ngh);
        }
      }
      if (plus) {
        return lists::contains(plus, src, ngh);
      }
      return false;
    }

    // f: (src, offset) -> A
    template <class F>
    void map_elms(uintV src, F f) const {
      size_t plus_size = 0;
      if (plus) {
        lists::map_array(plus, src, 0, f);
        plus_size = lists::node_size(plus);
      }
      auto get_aug_size = [&] (Node* a) -> uintV {
        return Tree::aug_val(a);
      };
      auto get_size = [&] (Node* a) -> uintV {
        auto nd = Tree::get_entry(a).second;
        size_t sz = 1 + lists::node_size(nd);
        return sz;
      };
      auto map_f = [&] (const Entry& entry, size_t start_offset) {
        const uintV& key = entry.first;
        f(key, plus_size + start_offset);
        AT* arr = entry.second;
        lists::map_array(arr, src, plus_size + start_offset + 1, f);
      };
      auto T = edge_list(); T.root = root;
      T.map_elms_entry(map_f, get_aug_size, get_size);
      T.root = nullptr;
    }

    template <class F>
    bool iter_elms_cond(uintV src, F f) const {
      bool res = false;
      if (plus) {
        res = lists::iter_elms_cond(plus, src, f);
      }
      if (!res && root) {
        auto iter_f = [&] (const Entry& entry) {
          uintV key = entry.first;
          if (f(key)) { return true; }
          AT* arr = entry.second;
          return lists::iter_elms_cond(arr, src, f);
        };
        auto T = edge_list(); T.root = root;
        res = T.iter_elms_cond(iter_f);
        T.root = nullptr;
      }
      return res;
    }


    template <class F>
    void iter_elms(uintV src, F f) const {
      bool res = false;
      if (plus) {
        lists::iter_elms(plus, src, f);
      }
      if (!res && root) {
        auto iter_f = [&] (const Entry& entry) {
          uintV key = entry.first;
          f(key);
          AT* arr = entry.second;
          lists::iter_elms(arr, src, f);
        };
        auto T = edge_list(); T.root = root;
        T.iter_elms(iter_f);
        T.root = nullptr;
      }
    }

    // f: (src, ngh) -> void
    template <class F>
    void map_nghs (uintV src, F& f) const {
      if (plus) {
        lists::map_nghs(plus, src, f);
      }
      auto get_aug_size = [&] (Node* a) -> uintV {
        return Tree::aug_val(a);
      };
      auto get_size = [&] (Node* a) -> uintV {
        auto nd = Tree::get_entry(a).second;
        return 1 + lists::node_size(nd);
      };
      auto map_f = [&] (const Entry& entry, size_t start_offset) {
        f(src, entry.first);
        lists::map_nghs(entry.second, src, f);
      };
      auto T = edge_list(); T.root = root;
      // TODO: don't need to map with idx.
      T.map_elms_entry(map_f, get_aug_size, get_size);
      T.root = nullptr;
    }

    template <class P>
    struct count_struct {
      using t = size_t;
      P p; uintV src;
      count_struct(P _p, uintV _src) : p(_p), src(_src) {}
      size_t identity() { return static_cast<size_t>(0); }
      size_t from_entry(const Entry& entry) {
        size_t ct = 0;
        ct += p(src, entry.first);
        ct += lists::count_pred(entry.second, src, p);
        return ct;
      }
      size_t combine(size_t l, size_t r) { return l + r; }
    };
    template <class P>
    static auto make_count_struct(P p, uintV src) { return count_struct<P>(p, src); }

    template <class P>
    size_t count_nghs(uintV src, P p) const {
      size_t ct = 0;
      if (plus) {
        ct += lists::count_pred(plus, src, p);
      }
      if (root) {
        auto MR = make_count_struct(p, src);
        ct += Tree::map_reduce(root, MR);
      }
      return ct;
    }


    struct uncompressed_size_struct {
      using t = size_t;
      uintV src;
      uncompressed_size_struct(uintV _src) : src(_src) {}
      size_t identity() { return static_cast<size_t>(0); }
      size_t from_entry(const Entry& entry) {
        size_t sz = 0;
        sz += lists::uncompressed_size(entry.second, src);
        return sz;
      }
      size_t combine(size_t l, size_t r) { return l + r; }
    };

    size_t uncompressed_size(uintV src) const {
      size_t sz = 0;
      if (plus) {
        sz += lists::uncompressed_size(plus, src);
      }
      if (root) {
        auto MR = uncompressed_size_struct(src);
        sz += Tree::map_reduce(root, MR);
      }
      return sz;
    }

    uintV* get_edges(uintV src) const {
      auto ret = pbbs::new_array_no_init<uintV>(degree());
      size_t i = 0;
      auto f = [&] (uintV ngh) {
        ret[i++] = ngh;
        return false;
      };
      iter_elms_cond(src, f);
      assert(i == degree());
      return ret;
    }

    void check_edge_ct(uintV src) const {
      size_t i = 0;
      auto f = [&] (uintV ngh) {
        i++;
        return false;
      };
      iter_elms_cond(src, f);
      if (i != degree()) {
        cout << "Failed on src = " << src << endl;
        assert(i == degree());
      }
    }


    struct size_struct {
      using t = size_t;
      uintV src;
      size_struct(uintV _src) : src(_src) {}
      size_t identity() { return static_cast<size_t>(0); }
      size_t from_entry(const Entry& entry) {
        return lists::underlying_array_size(entry.second);
      }
      size_t combine(size_t l, size_t r) { return l + r; }
    };

    size_t size_in_bytes(uintV src) const {
      size_t n_bytes = 0;
      if (plus) {
        n_bytes += lists::underlying_array_size(plus);
        assert(lists::get_ref_ct(plus) == 1);
      }
      if (root) {
        n_bytes += Tree::map_reduce(root, size_struct(src));
        assert(root->ref_cnt == 1);
      }
      return n_bytes;
    }

    struct num_nodes_struct {
      using t = size_t;
      num_nodes_struct() {}
      size_t identity() { return static_cast<size_t>(0); }
      size_t from_entry(const Entry& entry) {
        return 1;
      }
      size_t combine(size_t l, size_t r) { return l + r; }
    };

    size_t edge_tree_nodes() const {
      size_t n_nodes = 0;
      if (root) {
        n_nodes += Tree::map_reduce(root, num_nodes_struct());
      }
      return n_nodes;
    }

    struct check_ref_cts_struct {
      using t = size_t;
      uintV src;
      check_ref_cts_struct(uintV _src) : src(_src) {}
      size_t identity() { return static_cast<size_t>(0); }
      size_t from_node(const Tree::node* node) {
        return node->ref_cnt;
      }
      size_t combine(size_t l, size_t r) { return l + r; }
    };

    size_t check_ref_cts(uintV src) const {
      size_t n_refs = 0;
      if (root) {
        n_refs += Tree::map_reduce_node(root, check_ref_cts_struct(src));
      }
      return n_refs;
    }

    void check_consistency(uintV src) const {
      long last_vtx = -1;
      bool ok = true;
      auto iter_f = [&] (const Entry& entry, const Tree::node* node) {
        uintV cur_key = entry.first;

        if (cur_key < last_vtx) {
          cout << "Inversion when inspecting cur_key = " << cur_key << endl;
          ok = false;
        }
        last_vtx = cur_key;
        return false;
      };
      auto T = edge_list(); T.root = root;
      T.iter_nodes_cond(iter_f);
      T.root = nullptr;
      if (!ok) {
        print_structure(src);
        assert(false);
      }
    }

    void print_structure(uintV src) const {
      if (plus) {
        cout << "[plus: size = " << lists::node_size(plus) << "] ";
      } else {
        cout << "[no-plus] ";
      }
      if (root) {
        auto iter_f = [&] (const Entry& entry, const Tree::node* node) {
          uintV key = entry.first;
          AT* arr = entry.second;
          size_t ref_cnt = node->ref_cnt;
          size_t arr_size = (arr) ? (arr[0]) : 0;
          cout << "(" << key << "," << arr_size << "," << ref_cnt << ") ";
          return false;
        };
        auto T = edge_list(); T.root = root;
        T.iter_nodes_cond(iter_f);
        T.root = nullptr;
        cout << " total deg = " << degree() << endl;
      } else {
        cout << "[no-tree]" << " total deg = " << degree() << endl;
      }
    }

    treeplus(AT* _plus, Node* _root) : plus(_plus), root(_root) {
    }
    treeplus() : plus(nullptr), root(nullptr) {}


    // Treeplus constructor
    template <class SQ>
    treeplus(SQ const &S, uintV src, pbbs::flags fl = pbbs::no_flag) {
      if (S.size() == 0) {
        plus = nullptr; root = nullptr; return; //deg = 0; return;
      }

      bool run_seq = fl & pbbs::fl_sequential;

      // 1. Compute the index of each head
      constexpr const size_t start_array_size = 500;
      uintV starts_backing[start_array_size];
      uintV* starts = (S.size() > start_array_size) ? pbbs::new_array_no_init<uintV>(S.size()) : starts_backing;
      parallel_for(0, S.size(), [&] (size_t i) {
        if (lists::is_head(S[i])) starts[i] = i;
        else starts[i] = UINT_V_MAX;
      }, run_seq ? std::numeric_limits<long>::max() : 2000);

      // 2. Copy scan to map each elm to its head
      auto cpy = [&] (const uintV& l, const uintV& r) {
        if (r == UINT_V_MAX) { return l; }
        return r;
      };
      auto starts_seq = pbbs::make_range(starts, starts + S.size());
      auto cpy_monoid = pbbs::make_monoid(cpy, UINT_V_MAX);
      pbbs::scan_inplace(starts_seq, cpy_monoid, pbbs::fl_scan_inclusive | fl);

      // 3. Find the index of the first head (b probes in expectation)
      size_t first_ind = S.size();
      for (size_t i=0; i<S.size(); i++) {
        if (starts[i] != UINT_V_MAX) {
          first_ind = i;
          break;
        }
      }

      // 4. Generate the plus
      plus = lists::generate_plus(S, first_ind, src);

      // 5. Pack out the heads
      auto is_head_seq = pbbs::delayed_seq<bool>(S.size(), [&] (size_t i) { return lists::is_head(S[i]); });
      uintV* head_idxs = starts;
      size_t k = 0;
      for (size_t i=0; i<S.size(); i++) {
        if (is_head_seq[i]) head_idxs[k++] = i;
      }
      auto head_indices = pbbs::make_range(head_idxs, head_idxs+ k);
      using K = uintV; using V = AT*; using KV = pair<K, V>;
      constexpr const size_t kv_arr_size = 20;
      KV kv_stack[kv_arr_size];
      KV* kvs = (head_indices.size() > kv_arr_size) ? pbbs::new_array_no_init<KV>(head_indices.size()) : kv_stack;
      auto KV_seq = pbbs::make_range(kvs, kvs+head_indices.size());

      // 6. Generate each head node
      parallel_for(0, head_indices.size(), [&] (size_t i) {
        uintV head_index = head_indices[i];
        uintV key = S[head_index];
        get<0>(KV_seq[i]) = key;
        V& noderef = get<1>(KV_seq[i]);
        uintV start = head_index + 1; // don't include key
        uintV end = (i == (head_indices.size() - 1)) ? S.size() : head_indices[i+1];
        noderef = lists::generate_tree_node(key, src, S, start, end);
      }, run_seq ? std::numeric_limits<long>::max() : 5); // TODO: granularity

      // 7. insert all heads into the edges tree
      auto tree = edge_list::multi_insert_sorted(edge_list(), KV_seq); // run_seq
      root = tree.root; tree.root = nullptr;
//      deg = S.size();
      if (S.size() > start_array_size) { pbbs::free_array(starts); }
      if (head_indices.size() > kv_arr_size) { pbbs::free_array(kvs); }
    }
  };

  // Identical to a tree_plus; separating as this version of the object and
  // routines on it are really only used in split/union/difference/intersect.
  struct treeplus_c {
    using Tree = typename edge_list::Tree;
    using Node = typename edge_list::node;
    using Entry = typename edge_entry::entry_t; // pair<uintV, node>
    using Tree_GC = Tree::GC;

    AT* plus;
    Node* root;

    // Uses two reads to construct the degree instead of explicitly storing it.
    size_t degree() const {
      size_t deg = 0;
      if (plus) {
        deg += lists::node_size(plus);
      }
      if (root) {
        deg += Tree::aug_val(root);
      }
      return deg;
    }
    size_t size() const { return degree(); }


    bool has_tree() { return root != nullptr; }
    bool has_plus() { return plus != nullptr; }
    bool empty() { return root == nullptr && plus == nullptr; }

    uintV get_key() {
      assert(root);
      return Tree::get_key(root);
    }

    // inconsistent with leftmost_key, fix later.
    static inline maybe<Entry> rightmost(Node* node) {
      if (node) {
        return maybe<Entry>(Tree::get_entry(Tree::last(node)));
      }
      return maybe<Entry>();
    }

    static inline maybe<uintV> leftmost_key(Node* node) {
      if (node) {
        return maybe<uintV>(Tree::get_key(Tree::first(node)));
      }
      return maybe<uintV>();
    }

    inline maybe<uintV> get_leftmost_key() {
      return leftmost_key(root);
    }

    // look up edges
    bool contains(uintV src, uintV ngh) const {
      if (root) {
        auto mp = Tree::previous_or_eq(root, ngh);
        if (mp) {
          const Entry& entry = Tree::get_entry(mp);
          if (entry.first == ngh) return true;
          return lists::contains(entry.second, src, ngh);
        }
      }
      if (plus) {
        return lists::contains(plus, src, ngh);
      }
      return false;
    }

    treeplus_c copy_treeplus(uintV src) {
      AT* new_plus = nullptr;
      Node* new_root = nullptr;
      if (plus) {
        new_plus = lists::copy_node(plus); // bumps the ref-ct
      }
      if (root) {
        Tree_GC::increment(root);
        new_root = root;
      }
      auto ret = treeplus_c(new_plus, new_root);
      return ret;
    }

    void del() {
      lists::deallocate(plus);
      auto T = edge_list(); T.root = root;
    }

    static uintV get_last_key(AT* node, uintV src) {
      auto first_and_last = lists::first_and_last_keys(node, src);
      return get<1>(first_and_last);
    }

    void check_consistency(uintV src) const {
      long last_vtx = -1;
      bool ok = true;
      auto iter_f = [&] (const Entry& entry, const Tree::node* node) {
        uintV cur_key = entry.first;
        if (cur_key < last_vtx) {
          cout << "Inversion when inspecting cur_key = " << cur_key << endl;
          ok = false;
        }
        last_vtx = cur_key;
        return false;
      };
      auto T = edge_list(); T.root = root;
      T.iter_nodes_cond(iter_f);
      T.root = nullptr;
      if (!ok) {
        print_structure(src);
        assert(false);
      }
    }


    void print_structure(AT* plus, Node* root) const {
      if (plus) {
        cout << "[plus: size = " << lists::node_size(plus) << "] ";
      } else {
        cout << "[no-plus] ";
      }
      if (root) {
        auto iter_f = [&] (const Entry& entry, const Tree::node* node) {
          uintV key = entry.first;
          AT* arr = entry.second;
          size_t arr_size = (arr) ? (arr[0]) : 0;
          size_t ref_cnt = node->ref_cnt;
          cout << "(" << key << "," << arr_size << "," << ref_cnt << ") ";
          return false;
        };
        auto T = edge_list(); T.root = root;
        T.iter_nodes_cond(iter_f);
        T.root = nullptr;
        cout << "root ref-cnt = " << root->ref_cnt << " total deg = " << degree() << endl;
      } else {
        cout << "[no-tree] total deg = " << degree() << endl;
      }
    }

    void print_structure(uintV src) const {
      print_structure(plus, root);
    }

    treeplus_c(treeplus& T) {
      plus = T.plus;
      root = T.root;
    }
    treeplus_c(AT* _plus, Node* _root) : plus(_plus), root(_root) {
//      if (plus) {
//        deg += lists::node_size(plus);
//      }
//      if (root) {
//        auto T = edge_list();
//        T.root = root;
//        deg += T.aug_val();
//        T.root = nullptr;
//      }
    }
    treeplus_c() : plus(nullptr), root(nullptr) {}
  };


  using Tree = edge_list::Tree;
  using Node = edge_list::node;
  using Entry = typename edge_entry::entry_t; // pair<uintV, node>
  using Tree_GC = Tree::GC;

  Node* copy_tree_node(Node* node, uintV src) {
    if (node) {
      auto entry = Tree::get_entry(node);
      entry.second = lists::copy_node(entry.second);
      auto root = Tree::make_node(entry);
      Tree_GC::increment(node->lc); Tree_GC::increment(node->rc);
      return Tree::node_join(node->lc, node->rc, root);
    }
    return nullptr;
  }

  // split takes a treeplus and a splitting key and returns a pair:
  // treeplus * treeplus
  // (left is all  < key, right is all > key).
  struct split_info {
    split_info(treeplus_c first, treeplus_c second, bool removed)
      : first(first), second(second), removed(removed) {};
    treeplus_c first;
    treeplus_c second;
    bool removed;

    void print(uintV src) {
      cout << "=== splitinfo" << endl;
      first.print_structure(src);
      second.print_structure(src);
      cout << "split_key found: " << removed << endl;
    }
  };

  // Invariant: the plus's returned must be allocated.
  // Either returns a new node, or an unchanged node with a ref-cnt bump.
  static split_info split(treeplus_c T, const uintV& src, const uintV& split_key) {
    AT* plus = T.plus; Node* bst = T.root;
    if (T.empty()) { return split_info(T, T, false); }

    if (plus) {
      uintV first_key; uintV last_key;
      std::tie(first_key, last_key) =
        lists::first_and_last_keys(plus, src);
      assert(first_key != UINT_V_MAX); assert(last_key != UINT_V_MAX);
      if (first_key <= split_key && split_key <= last_key) { // splits plus
        AT* lt; AT* gt; bool found_key;
        std::tie(lt, gt, found_key) =
          lists::split_node(plus, src, split_key);
        Tree_GC::increment(bst);
        return split_info(treeplus_c(lt, nullptr),
                          treeplus_c(gt, bst), found_key);
      } else if (split_key < first_key) { // left of (plus, bst).
        auto cpy = lists::copy_node(plus);
        Tree_GC::increment(bst);
        return split_info(treeplus_c(nullptr, nullptr),
                          treeplus_c(cpy, bst), false);
      } else if (!bst) { // plus as either left_plus, or right_plus.
        auto cpy = lists::copy_node(plus);
        if (split_key < first_key) {
          return split_info(treeplus_c(nullptr, nullptr),
                            treeplus_c(cpy, nullptr), false);
        } else {
          assert(split_key > last_key);
          return split_info(treeplus_c(cpy, nullptr),
                            treeplus_c(nullptr, nullptr), false);
        }
      }
    }

    assert(bst);
    const auto& entry = Tree::get_entry(bst);
    uintV key = entry.first;
    if (key < split_key) {
      uintV last_key = treeplus_c::get_last_key(entry.second, src);
      if (last_key != UINT_V_MAX && split_key <= last_key) {
        auto attached_node = entry.second;
        AT* lt; AT* gt; bool found_key;
        std::tie(lt, gt, found_key) =
          lists::split_node(attached_node, src, split_key);

        Node* join = Tree::make_node(entry);
        auto E = Tree::get_entry(join);
        E.second = lt;
        Tree::set_entry(join, E);

        Tree_GC::increment(bst->lc);
        Tree_GC::increment(bst->rc);
        return split_info(treeplus_c(lists::copy_node(plus),
                                   Tree::node_join(bst->lc, nullptr, join)),
                          treeplus_c(gt, bst->rc), found_key);
      } else {
        Tree_GC::increment(bst->lc);
        Node* join = Tree::make_node(entry);
        auto E = Tree::get_entry(join);
        E.second = lists::copy_node(entry.second);
        Tree::set_entry(join, E);

        split_info bstpair = split(treeplus_c(nullptr, bst->rc), src, split_key);
        assert(bstpair.first.plus == nullptr);
        bstpair.first = treeplus_c(lists::copy_node(plus),
            Tree::node_join(bst->lc, bstpair.first.root, join));
        return bstpair;
      }
    } else if (key > split_key) {
      Tree_GC::increment(bst->rc);
      Node* join = Tree::make_node(entry);
      auto E = Tree::get_entry(join);
      E.second = lists::copy_node(entry.second);
      Tree::set_entry(join, E);

      split_info bstpair = split(treeplus_c(nullptr, bst->lc), src, split_key);
      assert(bstpair.first.plus == nullptr);
      bstpair.first = treeplus_c(lists::copy_node(plus),
          bstpair.first.root);
      bstpair.second = treeplus_c(bstpair.second.plus,
          Tree::node_join(bstpair.second.root, bst->rc, join));
      return bstpair;
    } else {
      Tree_GC::increment(bst->lc);
      Tree_GC::increment(bst->rc);

      auto lt_plus = lists::copy_node(plus);
      auto rt_plus = lists::copy_node(entry.second);
      return split_info(treeplus_c(lt_plus, bst->lc),
                        treeplus_c(rt_plus, bst->rc), true);
    }
  }

  // Union Invariant: returned value is only visible to the caller.
  // Writing all code purely functionally for now. Can add back extra_ptr checks
  // later.
  static treeplus_c union_bc(AT* b1_plus, treeplus_c b2, uintV src, bool run_seq) {
    if (b2.has_plus() && !b2.has_tree()) {
      return treeplus_c(lists::union_nodes(b1_plus, b2.plus, src), nullptr);
    }

    uintV b1_first; uintV b1_last;
    std::tie(b1_first, b1_last) = lists::first_and_last_keys(b1_plus, src);

    auto m_leftmost = b2.get_leftmost_key();
    uintV leftmost = m_leftmost.value; assert(m_leftmost.valid);

    AT* new_plus;
    if (leftmost > b1_last) {
      if (b2.plus) {
        new_plus = lists::union_nodes(b1_plus, b2.plus, src);
      } else {
        new_plus = lists::copy_node(b1_plus);
      }
      Tree_GC::increment(b2.root);
      return treeplus_c(new_plus, b2.root);
    }

    bool found_key;
    AT* to_insert = nullptr, *split_plus = nullptr;
    std::tie(split_plus, to_insert, found_key) =
      lists::split_node(b1_plus, src, leftmost);
    // can't be a found_key---otherwise it would not be in the plus.
    assert(!found_key);
    if (split_plus) {
      if (b2.plus) {
        new_plus = lists::union_nodes(split_plus, b2.plus, src);
        lists::deallocate(split_plus);
      } else {
        new_plus = split_plus;
      }
    } else {
      new_plus = lists::copy_node(b2.plus);
    }

    if (to_insert) {
      size_t size = lists::node_size(to_insert);
      using K = uintV; using V = AT*; using KV = pair<K, V>;
      static constexpr const size_t kv_thresh = 1024;
      KV kv_stack[kv_thresh];
      KV* kvs = (size > kv_thresh) ? pbbs::new_array_no_init<KV>(size) : kv_stack;

      size_t distinct_keys = 0;
      uintV last_key = UINT_V_MAX; size_t start = 0;
      auto it = lists::read_iter(to_insert, src);

      auto create_new_key = [&] (uintV key, size_t start, size_t end) {
        auto mk = Tree::find(b2.root, key);
        if (mk == nullptr) {
          b2.print_structure(src);
          cout << "Tried finding key = " << key << " start = " << start << " end = " << end << " src = " << src << endl;
        }
        assert(mk != nullptr);
        const auto& key_node = Tree::get_entry(mk);
        AT* key_arr = key_node.second;

        size_t deg = end - start;
        if (key_arr) {
          AT* new_arr = lists::union_it_and_node(it, deg, key_arr, src);
          kvs[distinct_keys++] = make_pair(key, new_arr);
        } else {
          kvs[distinct_keys++] = make_pair(key, lists::copy_from_it(it, deg, src));
        }
      };

      auto map_f = [&] (uintV ngh, size_t i) {
        auto mp = Tree::previous(b2.root, ngh); assert(mp != nullptr);
        uintV cur_key = Tree::get_entry(mp).first;
        if (cur_key != last_key) {
          if (last_key != UINT_V_MAX) {
            create_new_key(last_key, start, i);
          }
          start = i;
          last_key = cur_key;
        }
        if (i == size-1) {
          create_new_key(cur_key, start, size);
        }
      };
      lists::map_array(to_insert, src, 0, map_f);

      // insert kvs
      auto replace = [] (const V& a, const V& b) {
        lists::deallocate(a);
        return b;
      };
      auto new_root = Tree::multi_insert_sorted(b2.root, kvs, distinct_keys, replace, true);

      lists::deallocate(to_insert);
      if (distinct_keys > kv_thresh) pbbs::free_array(kvs);
      auto ret = treeplus_c(new_plus, new_root);
      return ret;
    }

    // Otherwise, if to_insert is empty then take new_plus as our plus and
    // return the same root (incrementing ref-cnt)
    Tree_GC::increment(b2.root);
    auto ret = treeplus_c(new_plus, b2.root);
    return ret;
  }

  // Union Invariant: returned value is only visible to the caller.
  static treeplus_c union_rec(treeplus_c b1, treeplus_c b2, uintV src, bool run_seq) {
    if (b1.empty()) return b2.copy_treeplus(src);
    if (b2.empty()) return b1.copy_treeplus(src);

    if (b1.has_plus() && !b1.has_tree())
      return union_bc(b1.plus, b2, src, run_seq);
    if (b2.has_plus() && !b2.has_tree())
      return union_bc(b2.plus, b1, src, run_seq);

    size_t n1 = b1.size(); size_t n2 = b2.size();
    Node* root = Tree::make_node(Tree::get_entry(b2.root)); // copy
    const Entry& old_entry = Tree::get_entry(root);
    AT* root_arr = old_entry.second;

    Entry new_entry = Tree::get_entry(root);
    new_entry.second = nullptr;

    uintV split_key = b2.get_key();
    split_info bsts = split(b1, src, split_key);
    assert(bsts.first.degree() + bsts.second.degree() + ((int)bsts.removed) == b1.degree());

    // Split root_arr by leftmost(bsts.second).
    // Update new_entry.second if we split the root_arr.
    AT* b2_rightplus = nullptr;
    bool split_root_arr = false;
    uintV leftmost = UINT_V_MAX;
    if (root_arr) {
      maybe<uintV> mb1_leftmost = bsts.second.get_leftmost_key();
      if (mb1_leftmost.valid) {
        leftmost = mb1_leftmost.value;
        uintV last_key = treeplus_c::get_last_key(root_arr, src);
        if (leftmost <= last_key) { // some of b2.entry arr's nghs must go right.
          bool found_key;
          std::tie(new_entry.second, b2_rightplus, found_key) =
            lists::split_node(root_arr, src, leftmost);
          split_root_arr = true;
        }
      }
    }

    // Split bsts.second.plus by leftmost(root->rc)
    AT* b1_to_root_arr = bsts.second.plus;
    bool split_bsts_second_plus = false;
    if (bsts.second.plus && b2.root->rc) {
      maybe<uintV> m_leftmost = treeplus_c::leftmost_key(b2.root->rc);
      if (m_leftmost.valid) {
        uintV leftmost = m_leftmost.value;
        uintV last_key = treeplus_c::get_last_key(bsts.second.plus, src);
        if (leftmost <= last_key) { // some of bsts.second.plus nghs must go right.
          bool found_key;
          AT* b1_rightplus = nullptr;
          std::tie(b1_to_root_arr, b1_rightplus, found_key) =
            lists::split_node(bsts.second.plus, src, leftmost);
          split_bsts_second_plus = true;
          if (b1_rightplus) {
            lists::deallocate(bsts.second.plus);
            bsts.second.plus = b1_rightplus;
          }
        }
      }
    }
    if (!split_bsts_second_plus)
      bsts.second.plus = nullptr;

    // Form the root's new array.
    if (b1_to_root_arr) {
      if (new_entry.second) { // join with the split portion or root_arr.
        auto to_del = new_entry.second;
        new_entry.second = lists::union_nodes(new_entry.second, b1_to_root_arr, src);
        lists::deallocate(to_del); lists::deallocate(b1_to_root_arr);
      } else if (!split_root_arr && root_arr) { // root_arr was not split, join with it.
        new_entry.second = lists::union_nodes(root_arr, b1_to_root_arr, src);
        lists::deallocate(b1_to_root_arr);
      } else { // root_arr was empty, or was emptied into b2_rightplus
        new_entry.second = b1_to_root_arr;
      }
    } else if (!split_root_arr) {
      new_entry.second = lists::copy_node(root_arr);
    }

    auto b2_left = treeplus_c(b2.plus, b2.root->lc);
    auto b2_right = treeplus_c(b2_rightplus, b2.root->rc);
    bsts.first = treeplus_c(bsts.first.plus, bsts.first.root);
    bsts.second = treeplus_c(bsts.second.plus, bsts.second.root);

    auto P = utils::fork<treeplus_c>(!run_seq && utils::do_parallel(n1, n2),
      [&] () {return union_rec(bsts.first, b2_left, src, run_seq);},
      [&] () {return union_rec(bsts.second, b2_right, src, run_seq);});

    lists::deallocate(b2_rightplus);
    bsts.first.del();
    bsts.second.del();

    // Invariant: Rec call copied, so we can use the plus.
    AT* new_plus = P.first.plus;
    Node* new_left = P.first.root;
    Node* new_right = P.second.root;

    assert(!P.second.plus);

    // Update entry, join and return.
    Tree::set_entry(root, new_entry);
    auto rr = Tree::node_join(new_left, new_right, root);
    return treeplus_c(new_plus, rr);
  }

  // Purely functional version
  static treeplus uniont(const treeplus& b1, const treeplus& b2, uintV src, bool run_seq=false) {
#ifdef CHECK_CORRECTNESS
    b1.check_edge_ct(src);
    b2.check_edge_ct(src);
#endif

    auto ret = union_rec(treeplus_c(b1.plus, b1.root), treeplus_c(b2.plus, b2.root), src, run_seq);
    auto tp = treeplus(ret.plus, ret.root);

#ifdef CHECK_CORRECTNESS
    auto b1_edges = b1.get_edges(src);
    auto b2_edges = b2.get_edges(src);
    auto tp_edges = tp.get_edges(src);
    uintV* ver_edges; uintV ver_deg;
    std::tie(ver_edges, ver_deg) = uncompressed_lists::union_2(b1_edges, b1.degree(), b2_edges, b2.degree());
    assert(ver_deg = tp.degree());
    pbbs::free_array(b1_edges); pbbs::free_array(b2_edges); pbbs::free_array(tp_edges);
    pbbs::free_array(ver_edges);
#endif
    return tp;
  }

//  // Never used expose? Check.
//  static tuple<treeplus_c, uintV, treeplus_c> expose(treeplus_c T) {
//  }

  // extra_b1:
  static Node* join2_i(Node* b1, Node* b2, bool extra_b1, bool extra_b2) {
    if (!b1) return Tree_GC::inc_if(b2, extra_b2);
    if (!b2) return Tree_GC::inc_if(b1, extra_b1);

    if (Tree::size(b1) > Tree::size(b2)) {
      bool copy_b1 = extra_b1 || b1->ref_cnt > 1;
      Node* l = Tree_GC::inc_if(b1->lc, copy_b1);
      Node* r = join2_i(b1->rc, b2, copy_b1, extra_b2);
      return Tree::node_join(l, r, Tree_GC::copy_if(b1, copy_b1, extra_b1));
    } else {
      bool copy_b2 = extra_b2 || b2->ref_cnt > 1;
      Node* l = join2_i(b1, b2->lc, extra_b1, copy_b2);
      Node* r = Tree_GC::inc_if(b2->rc, copy_b2);
      return Tree::node_join(l, r, Tree_GC::copy_if(b2, copy_b2, extra_b2));
    }
  }

  static treeplus_c join_plus_and_other(treeplus_c left, treeplus_c right, uintV src) {
    Tree_GC::increment(right.root);
    return treeplus_c(lists::union_nodes(left.plus, right.plus, src), right.root);
  }

  static treeplus_c join_other_and_plus(treeplus_c b1, treeplus_c b2, uintV src) {
    auto mr = b1.rightmost(b1.root); assert(mr.valid);
    uintV right_key = mr.value.first; auto right_arr = mr.value.second;

    auto new_entry = mr.value;
    new_entry.first = right_key;
    new_entry.second = lists::union_nodes(right_arr, b2.plus, src);
    auto replace = [] (AT* a, AT* b) {
      lists::deallocate(a);
      return b;
    };
    auto new_b1_root = Tree::insert(b1.root, new_entry, replace, true);
    return treeplus_c(lists::copy_node(b1.plus), new_b1_root);
  }


  Node* insert_rightmost(Node* root, AT* to_insert, uintV src) {
    auto mr = treeplus_c::rightmost(root); assert(mr.valid);
    uintV right_key = mr.value.first; auto right_arr = mr.value.second;

    auto new_entry = mr.value;
    new_entry.first = right_key;
    new_entry.second = lists::union_nodes(right_arr, to_insert, src);
    auto replace = [] (AT* a, AT* b) {
      lists::deallocate(a);
      return b;
    };
    auto new_root = Tree::insert(root, new_entry, replace, true);
    return new_root;
  }

  // join2: (treeplus * treeplus) -> treeplus
  //        (L * R) -> res
  // invariant: everything in L is < everything in R
  // idea: take the root of one of the trees, say L.
  //
  // base cases:
  //  (plus, nullptr) * (_, _) -> (plus, tree) bc
  //  (_, tree) * (plus, nullptr) -> (tree, plus) bc
  //  (tree * tree) -> recurse
  //
  //  Implemented purely functionally---caller should del() the two supplied
  //  treeplusses
  static treeplus_c join2(treeplus_c b1, treeplus_c b2, uintV src) {
    if (b1.empty()) return b2.copy_treeplus(src);
    if (b2.empty()) return b1.copy_treeplus(src);

    if (b1.has_plus() && !b1.has_tree())
      return join_plus_and_other(b1, b2, src);
    if (b2.has_plus() && !b2.has_tree())
      return join_other_and_plus(b1, b2, src);

    // if we insert and create a new node, we had better decr.
    auto new_b1_root = b1.root;
    bool copied_b1 = false;
    if (b2.plus) {
      new_b1_root = insert_rightmost(b1.root, b2.plus, src);
      copied_b1 = true;
    }

    auto new_root = join2_i(new_b1_root, b2.root, /*extra_b1=*/true, /*extra_b2=*/true);
    if (copied_b1) {
      // Must be decrement_recursive to fix ref-cnts correctly.
      Tree_GC::decrement_recursive(new_b1_root);
    }
    auto ret = treeplus_c(lists::copy_node(b1.plus), new_root);
    return ret;
  }



  // b1: plus, b2: (plus | tree)
  static treeplus_c difference_bc_1(treeplus_c b1, treeplus_c b2, uintV src, bool run_seq) {
    if (b2.has_plus() && !b2.has_tree()) {
      return treeplus_c(lists::difference(b1.plus, b2.plus, src), nullptr);
    }
    // b2: = (plus + tree)

    uintV b1_first; uintV b1_last;
    std::tie(b1_first, b1_last) = lists::first_and_last_keys(b1.plus, src);

    auto m_leftmost = b2.get_leftmost_key();
    uintV leftmost = m_leftmost.value; assert(m_leftmost.valid);

    AT* new_plus = nullptr;
    if (leftmost > b1_last) {
      if (b2.plus) {
        new_plus = lists::difference(b1.plus, b2.plus, src);
      }
      Tree_GC::increment(b2.root);
      return treeplus_c(new_plus, b2.root);
    }

    bool found_key;
    AT* to_insert = nullptr, *split_plus = nullptr;
    std::tie(split_plus, to_insert, found_key) =
      lists::split_node(b1.plus, src, leftmost);
    // can't be a found_key---otherwise it would not be in the plus.
    assert(!found_key);
    if (split_plus) {
      if (b2.plus) {
        new_plus = lists::difference(split_plus, b2.plus, src);
      }
      lists::deallocate(split_plus);
    } else {
      new_plus = lists::copy_node(b2.plus);
    }

    if (to_insert) {
      size_t size = lists::node_size(to_insert);
      using K = uintV; using V = AT*; using KV = pair<K, V>;
      static constexpr const size_t kv_thresh = 1024;
      KV kv_stack[kv_thresh];
      KV* kvs = (size > kv_thresh) ? pbbs::new_array_no_init<KV>(size) : kv_stack;

      size_t distinct_keys = 0;
      uintV last_key = UINT_V_MAX; size_t start = 0;
      auto it = lists::read_iter(to_insert, src);

      auto create_new_key = [&] (uintV key, size_t k_start, size_t k_end) {
        auto mk = Tree::find(b2.root, key);
        assert(mk != nullptr);
        const auto& key_node = Tree::get_entry(mk);
        AT* key_arr = key_node.second;

        size_t deg = k_end - k_start;
        if (key_arr) {
          AT* new_arr = lists::difference_it_and_node(it, deg, key_arr, src);
          kvs[distinct_keys++] = make_pair(key, new_arr);
        } else {
          // skip deg many iterands.
          // todo: can optimize out this update
          for (size_t i=0; i<deg; i++) it.next();
          kvs[distinct_keys++] = make_pair(key, nullptr);
        }
      };

      auto map_f = [&] (uintV ngh, size_t i) {
        auto mp = Tree::previous(b2.root, ngh); assert(mp != nullptr);
        uintV cur_key = Tree::get_entry(mp).first;
        if (cur_key != last_key) {
          if (last_key != UINT_V_MAX) {
            create_new_key(last_key, start, i);
          }
          start = i;
          last_key = cur_key;
        }
        if (i == size-1) {
          create_new_key(cur_key, start, size);
        }
      };
      lists::map_array(to_insert, src, 0, map_f);

      // insert kvs
      auto replace = [] (const V& a, const V& b) {
        lists::deallocate(a);
        return b;
      };
      auto new_root = Tree::multi_insert_sorted(b2.root, kvs, distinct_keys, replace, true); // removed run_seq

      lists::deallocate(to_insert);
      if (distinct_keys > kv_thresh) pbbs::free_array(kvs);
      auto ret = treeplus_c(new_plus, new_root);
      return ret;
    }

    // Otherwise, if to_insert is empty then take new_plus as our plus and
    // return the same root (incrementing ref-cnt)
    Tree_GC::increment(b2.root);
    auto ret = treeplus_c(new_plus, b2.root);
    return ret;
  }

  // b1: tree, b2: plus
  // plus stays a plus. for each elm in plus, look it up in the tree,
  // and remove if it exists.
  static treeplus_c difference_bc_2(treeplus_c b1, treeplus_c b2, uintV src, bool run_seq) {
    assert(!b2.has_tree());
    auto filter_f = [&] (uintV ngh) {
      return !b1.contains(src, ngh);
    };
    return treeplus_c(lists::filter(b2.plus, src, filter_f), nullptr);
  }


  // Returns b2 / b1
  // Assumes that b1 and b2 are owned by the caller.
  static treeplus_c difference_rec(treeplus_c b1, treeplus_c b2, uintV src, bool run_seq) {
    if (b1.empty()) return b2.copy_treeplus(src);
    if (b2.empty()) return treeplus_c(nullptr, nullptr);

    if (b1.has_plus() && !b1.has_tree())
      return difference_bc_1(b1, b2, src, run_seq);

    if (b2.has_plus() && !b2.has_tree())
      return difference_bc_2(b1, b2, src, run_seq);

    size_t n1 = b1.size(); size_t n2 = b2.size();

    Entry new_entry = Tree::get_entry(b2.root);
    auto root_arr = new_entry.second;
    new_entry.second = nullptr;

    uintV split_key = b2.get_key();
    split_info bsts = split(b1, src, split_key);
    assert(bsts.first.degree() + bsts.second.degree() + ((int)bsts.removed) == b1.degree());

    // Split root_arr by leftmost(bsts.second).
    // Update new_entry.second if we split the root_arr.
    AT* b2_rightplus = nullptr;
    bool split_root_arr = false;
    uintV leftmost = UINT_V_MAX;
    if (root_arr) {
      maybe<uintV> mb1_leftmost = bsts.second.get_leftmost_key();
      if (mb1_leftmost.valid) {
        leftmost = mb1_leftmost.value;
        uintV last_key = treeplus_c::get_last_key(root_arr, src);
        if (leftmost <= last_key) { // some of b2.entry arr's nghs must go right.
          bool found_key;
          std::tie(new_entry.second, b2_rightplus, found_key) =
            lists::split_node(root_arr, src, leftmost);
          split_root_arr = true;
        }
      }
    }

    // Split bsts.second.plus by leftmost(root->rc)
    AT* b1_to_root_arr = bsts.second.plus;
    bool split_bsts_second_plus = false;
    if (bsts.second.plus && b2.root->rc) {
      maybe<uintV> m_leftmost = treeplus_c::leftmost_key(b2.root->rc);
      if (m_leftmost.valid) {
        uintV leftmost = m_leftmost.value;
        uintV last_key = treeplus_c::get_last_key(bsts.second.plus, src);
        if (leftmost <= last_key) { // some of bsts.second.plus nghs must go right.
          bool found_key;
          AT* b1_rightplus = nullptr;
          std::tie(b1_to_root_arr, b1_rightplus, found_key) =
            lists::split_node(bsts.second.plus, src, leftmost);
          split_bsts_second_plus = true;
          if (b1_rightplus) {
            lists::deallocate(bsts.second.plus);
            bsts.second.plus = b1_rightplus;
          }
        }
      }
    }
    if (!split_bsts_second_plus)
      bsts.second.plus = nullptr;

    // Form the root's new array.
    if (b1_to_root_arr) {
      if (new_entry.second) { // difference with the split portion or root_arr.
        auto to_del = new_entry.second;
        new_entry.second = lists::difference(b1_to_root_arr, new_entry.second, src);
        lists::deallocate(to_del); lists::deallocate(b1_to_root_arr);
      } else if (!split_root_arr && root_arr) { // root_arr was not split, difference.
        new_entry.second = lists::difference(b1_to_root_arr, root_arr, src);
        lists::deallocate(b1_to_root_arr);
      } else { // root_arr was empty, or was emptied into b2_rightplus
        new_entry.second = nullptr;
        lists::deallocate(b1_to_root_arr);
      }
    } else if (!split_root_arr) {
      new_entry.second = lists::copy_node(root_arr);
    }

    auto b2_left = treeplus_c(b2.plus, b2.root->lc);
    auto b2_right = treeplus_c(b2_rightplus, b2.root->rc);
    bsts.first = treeplus_c(bsts.first.plus, bsts.first.root);
    bsts.second = treeplus_c(bsts.second.plus, bsts.second.root);

    auto P = utils::fork<treeplus_c>(!run_seq && utils::do_parallel(n1, n2),
      [&] () {return difference_rec(bsts.first, b2_left, src, run_seq);},
      [&] () {return difference_rec(bsts.second, b2_right, src, run_seq);});

    // Could delete some nodes in the right-call, causing a right-plus to be
    // formed. Depending on whether the root is deleted, this either gets hooked
    // into the root's array, or should be sent to the rightmost node in L, or
    // in the event that L is also empty, be joined with the returned plus.

    lists::deallocate(b2_rightplus);
    bsts.first.del();
    bsts.second.del();

    // Invariant: Rec call copied, so we can use the plus.
    AT* new_plus = P.first.plus;
    Node* new_left = P.first.root;
    Node* new_right = P.second.root;

    // if we have join3, the second branch is also pretty nice. sig is:
    // treeplus * (uintV, array) * treeplus -> treeplus

    if (P.second.plus) {
      auto old = new_entry.second;
      new_entry.second = lists::union_nodes(old, P.second.plus, src);
      lists::deallocate(old); lists::deallocate(P.second.plus);
    }

    if (bsts.removed) {
      auto l_plus = treeplus_c(new_plus, new_left);
      auto r_plus = treeplus_c(new_entry.second, new_right);

      // purely functional---increments if nec. and copies the plusses.
      auto ret = join2(l_plus, r_plus, src);

      l_plus.del(); r_plus.del();

      return ret;
    } else {
      Node* root = Tree::make_node(Tree::get_entry(b2.root));
      Tree::set_entry(root, new_entry);
      auto rr = Tree::node_join(new_left, new_right, root);
      return treeplus_c(new_plus, rr);
    }
  }

  // Purely functional version
  // Returns b2 / b1
  static treeplus difference(const treeplus& b1, const treeplus& b2, uintV src, bool run_seq=false) {
//    cout << "Difference: " << b1.degree() << " " << b2.degree() << " src = " << src << endl;
#ifdef CHECK_CORRECTNESS
    b1.check_edge_ct(src);
    b2.check_edge_ct(src);
#endif

    auto ret = difference_rec(treeplus_c(b1.plus, b1.root), treeplus_c(b2.plus, b2.root), src, run_seq);
    auto tp = treeplus(ret.plus, ret.root);

//    tp.print_structure(src);
#ifdef CHECK_CORRECTNESS
    auto b1_edges = b1.get_edges(src);
    auto b2_edges = b2.get_edges(src);
    auto tp_edges = tp.get_edges(src);
    uintV* ver_edges; uintV ver_deg;
    std::tie(ver_edges, ver_deg) = uncompressed_lists::difference_2(b1_edges, b1.degree(), b2_edges, b2.degree());
//    cout << "ver_deg = " << ver_deg << endl;
    if (ver_deg != tp.degree()) {
      cout << "src = " << src << " ver_deg = " << ver_deg << " to = " << tp.degree() << endl;
      cout << "b1.size = " << b1.degree() << " b2.size = " << b2.degree() << " src = " << src << endl;
      b1.print_structure(src);
      b2.print_structure(src);
      cout << "TP: " << endl;
      tp.print_structure(src);
      assert(ver_deg == tp.degree());
    }
    for (size_t i=0; i<tp.degree(); i++) {
      assert(tp_edges[i] == ver_edges[i]);
    }
    pbbs::free_array(b1_edges); pbbs::free_array(b2_edges); pbbs::free_array(tp_edges);
    pbbs::free_array(ver_edges);
#endif
//    cout << "In difference, ret size: " << tp.degree() << endl;
    return tp;
  }

  static size_t intersect_bc(treeplus_c b1, uintV s1, treeplus_c b2, uintV s2) {
    assert(b1.root == nullptr);
    if (b2.has_plus() && !b2.has_tree()) {
      return lists::intersect(b1.plus, s1, b2.plus, s2);
    }

    size_t ct = lists::intersect(b1.plus, s1, b2.plus, s2);

    uintV b1_first; uintV b1_last;
    std::tie(b1_first, b1_last) = lists::first_and_last_keys(b1.plus, s1);

    auto m_leftmost = b2.get_leftmost_key();
    uintV leftmost = m_leftmost.value; assert(m_leftmost.valid);

    if (leftmost > b1_last) return ct;

    bool found_key;
    AT* split_plus = nullptr, *into_tree = nullptr;
    std::tie(split_plus, into_tree, found_key) =
      lists::split_node(b1.plus, s1, leftmost);
    lists::deallocate(split_plus);
    ct += found_key;

    if (into_tree) {
      auto map_f = [&] (uintV ngh, size_t i) {
        if (Tree::find(b2.root, ngh) != nullptr) {
          ct++;
        } else {
          auto mp = Tree::previous(b2.root, ngh); assert(mp != nullptr);
          AT* arr = Tree::get_entry(mp).second;
          ct += lists::contains(arr, s2, ngh);
        }
      };
      lists::map_array(into_tree, s1, 0, map_f);
      lists::deallocate(into_tree);
    }
    return ct;
  }

  static size_t intersect_rec(treeplus_c b1, uintV s1, treeplus_c b2, uintV s2) {
    if (b1.empty() || b2.empty()) return static_cast<size_t>(0);
    if (b1.has_plus() && !b1.has_tree())
      return intersect_bc(b1, s1, b2, s2);
    if (b2.has_plus() && !b2.has_tree())
      return intersect_bc(b2, s2, b1, s1);

    size_t ct = 0;
    size_t n1 = b1.size(); size_t n2 = b2.size();

    uintV split_key = b2.get_key();
    split_info bsts = split(b1, s1, split_key);
    assert(bsts.first.degree() + bsts.second.degree() + ((int)bsts.removed) == b1.degree());
    ct += bsts.removed;

    AT* root_arr = Tree::get_entry(b2.root).second;

    // Split root's arr
    AT* root_to_right_plus = nullptr;
    AT* root_lt = root_arr;
    bool split_root_arr = false;
    uintV leftmost = UINT_V_MAX;
    if (root_arr) {
      maybe<uintV> mb1_leftmost = bsts.second.get_leftmost_key();
      if (mb1_leftmost.valid) {
        leftmost = mb1_leftmost.value;
        uintV last_key = treeplus_c::get_last_key(root_arr, s2);
        if (leftmost <= last_key) { // some of b2.entry arr's nghs must go right.
          bool found_key;
          std::tie(root_lt, root_to_right_plus, found_key) =
            lists::split_node(root_arr, s2, leftmost);
          split_root_arr = true;
          ct += found_key;
        }
      }
    }

    // Split bsts.second.plus
    AT* b1_to_root = bsts.second.plus;
    bool split_bsts_second_plus = false;
    if (bsts.second.plus && b2.root->rc) {
      maybe<uintV> m_leftmost = treeplus_c::leftmost_key(b2.root->rc);
      if (m_leftmost.valid) {
        uintV leftmost = m_leftmost.value;
        uintV last_key = treeplus_c::get_last_key(bsts.second.plus, s1);
        if (leftmost <= last_key) { // some of bsts.second.plus nghs must go right.
          bool found_key;
          AT* b1_rightplus = nullptr;
          std::tie(b1_to_root, b1_rightplus, found_key) =
            lists::split_node(bsts.second.plus, s1, leftmost);
          ct += found_key;
          split_bsts_second_plus = true;
          if (b1_rightplus) {
            lists::deallocate(bsts.second.plus);
            bsts.second.plus = b1_rightplus;
          }
        }
      }
    }
    if (!split_bsts_second_plus)
      bsts.second.plus = nullptr;

    ct += lists::intersect(root_lt, s2, b1_to_root, s1);
    if (split_root_arr) lists::deallocate(root_lt);
    lists::deallocate(b1_to_root);

    auto b2_left = treeplus_c(b2.plus, b2.root->lc);
    auto b2_right = treeplus_c(root_to_right_plus, b2.root->rc);
    bsts.first = treeplus_c(bsts.first.plus, bsts.first.root);
    bsts.second = treeplus_c(bsts.second.plus, bsts.second.root);

    auto P = utils::fork<size_t>(utils::do_parallel(n1, n2),
      [&] () {return intersect_rec(bsts.first, s1, b2_left, s2);},
      [&] () {return intersect_rec(bsts.second, s1, b2_right, s2);});

    bsts.first.del();
    bsts.second.del();
    lists::deallocate(root_to_right_plus);

    ct += P.first + P.second;
    return ct;
  }

  static size_t intersect(treeplus& b1, uintV s1, treeplus& b2, uintV s2) {
    size_t check_size = intersect_rec(treeplus_c(b1.plus, b1.root), s1, treeplus_c(b2.plus, b2.root), s2);

#ifdef CHECK_CORRECTNESS
    auto b1_edges = b1.get_edges(s1);
    auto b2_edges = b2.get_edges(s2);
    size_t ins_size = uncompressed_lists::intersect(b1_edges, b1.degree(), b2_edges, b2.degree());
    cout << "intersect = " << check_size << " correct size = " << ins_size << endl;
    assert(ins_size == check_size);
#endif

    return check_size;
  }

};
