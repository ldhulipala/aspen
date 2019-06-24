#pragma once

#include "tree_plus/immutable_graph_tree_plus.h"
#include "traversible_graph.h"

#include "versioning_utils.h"

#include <limits>


#define MFENCE __sync_synchronize


template <class snapshot_graph>
struct versioned_graph {

  using Node = typename snapshot_graph::Node;
  using Node_GC = typename snapshot_graph::Node_GC;

  struct version {
    snapshot_graph graph;
    version(snapshot_graph _graph) : graph(_graph) {}
  };

  // ================================== Start Wait-Free Code ==================================
  using status = uint64_t;
  using announcement = uint64_t;

  // Data for wait-free maintenance
  static constexpr size_t padding = 4; // 128 uint64_t's/ptrs
  size_t num_threads;
  volatile status current_version = ops::EMPTY;


  struct alignas(16) thread_data {
    announcement announce;
    status stat;
    Node* version;
  };

  thread_data* tdata;

  void collect(size_t id, Node* old_nd) {
    size_t offset = id*padding;
    Node* nd = tdata[offset].version; //ptr
    assert(nd == old_nd); // should be the same pointer.
    if (!nd) return;
    if (__sync_bool_compare_and_swap(&(tdata[offset].version), old_nd, nullptr)) {
      // we got the handle to this version; GC it.
      bool res = Node_GC::decrement_recursive(old_nd);
      debug(assert(res));
    }
  }

  void set(Node* t) {
    debug(cout << "setting new version to: " << ((size_t)t) << endl;);
    assert(t->ref_cnt == 1);

    status new_ver = ops::EMPTY;
    bool set_val = false;
    // Searches for an empty slot.
    for (size_t i = 0; i < num_threads; i++) {
      size_t offset = i*padding;
      if (tdata[offset].version == nullptr) {
        new_ver = ops::combine(i, ops::get_timestamp(current_version)+1, 0);
        tdata[offset].stat = ops::combine(new_ver, 0);
        tdata[offset].version = t; // put your new data in slot
        debug(cout << "set version at offset = " << offset << endl;);
        set_val = true;
        break;
      }
    }
    if (!set_val) {
      cout << "Could not set val!!!!" << endl;
      assert(false);
      exit(0);
    }
    current_version = new_ver;
    debug(cout << "new_timestamp = " << ops::get_timestamp(current_version) << endl;);

    // Help
    for(size_t i = 0; i < num_threads; i++) {
      size_t offset = i*padding;
      for(size_t j = 0; j < 3; j++) {
        announcement ann = tdata[offset].announce;
        if (ops::get_flag(ann)) {
          pbbs::atomic_compare_and_swap(&(tdata[offset].announce), ann, ops::combine(new_ver, 0));
        }
      }
    }
  }

  // Acquires a new version for this thread.
  size_t acquire() {

    int id = worker_id();
    size_t offset = id*padding;

    announcement old = ops::combine(ops::EMPTY, 1);
    tdata[offset].announce = old;
    MFENCE();

    status cur = current_version;
    for (int i = 0; i < 2; i++) {
      if (!pbbs::atomic_compare_and_swap(&((tdata[offset].announce)), old, ops::combine(cur, 1))) {
        return ops::get_idx(tdata[offset].announce);
      }
      old = ops::combine(cur, 1);
      // read again
      status new_cur = current_version;
      if (cur == new_cur) { //  read a stable version
        pbbs::atomic_compare_and_swap(&(tdata[offset].announce), old, ops::combine(cur, 0));
        return ops::get_idx(tdata[offset].announce);
      }
      // read a newer version
      cur = new_cur;
    }

    return ops::get_idx(tdata[offset].announce);
  }


  // Acquires a new version for the updater.
  size_t acquire_update() {
    int id = worker_id();
    tdata[padding*id].announce = ops::combine(current_version, 0);
    return ops::get_idx(current_version);
  }


  // Releases this thread's version.
  void release() {
    int id = worker_id();
    size_t offset = id*padding;
    status v = ops::get_version(tdata[offset].announce);
    tdata[offset].announce = ops::EMPTY;
    MFENCE();
    if (v == current_version) {
      return; // someone else will handle it
    }
    status s = tdata[padding*ops::get_idx(v)].stat;

    if (v != ops::get_version(s)){
      return;
    }

    if (ops::get_flag(s) == 0) {
      if (!pbbs::atomic_compare_and_swap(&(tdata[padding*ops::get_idx(v)].stat), s, ops::combine(ops::get_version(s), 1))) {
        return;
      }
      for (int i = 0; i < num_threads; i++) {
        size_t offset = i*padding;
        if (tdata[offset].announce == ops::combine(v,1)) {
            pbbs::atomic_compare_and_swap(&tdata[offset].announce, ops::combine(v,1), ops::combine(v,0));
        }
      }
      s = ops::combine(ops::get_version(s), 2);
      tdata[padding*ops::get_idx(v)].stat = s;
      MFENCE();
    }
    if (ops::get_flag(s) == 2) {
      for (int i = 0; i < num_threads; i++) {
        size_t offset = i*padding;
        if (tdata[offset].announce == ops::combine(v,0)) {
          return;
        }
      }
      if (pbbs::atomic_compare_and_swap(&(tdata[padding*ops::get_idx(v)].stat), s, ops::EMPTY)) {
        collect(ops::get_idx(v), tdata[padding*ops::get_idx(v)].version);
      }
    }
  }
  // ================================== End Wait-Free Code =====================================


  void init_data() {
    num_threads = std::thread::hardware_concurrency();
    tdata = pbbs::new_array<thread_data>(num_threads*padding);
    for (size_t i=0; i<num_threads; i++) {
      tdata[i*padding].version = nullptr;
      tdata[i*padding].stat = ops::EMPTY;
      tdata[i*padding].announce = ops::EMPTY;
    }
  }

  versioned_graph(size_t n, size_t m, uintE* offsets, uintV* edges) {
    init_data();

    auto G = snapshot_graph(n, m, offsets, edges);
    assert(G.get_root()->ref_cnt == 1);

    set(G.get_root());
    G.clear_root();
    debug(cout << "Finished build" << endl;);
  }

  versioned_graph(snapshot_graph&& G) {
    init_data();

    set(G.get_root());
    assert(G.get_root()->ref_cnt == 1);
    G.clear_root();
    debug(cout << "Finished build (move)" << endl;);
  }

  versioned_graph() {
    init_data();
  }

  version acquire_version() {
    debug(cout << "== Acquire version" << endl;);
    size_t idx = acquire();
    debug(cout << "  idx = " << idx << endl;);
    auto root = tdata[idx*padding].version;
    debug(cout << "  root = " << ((size_t)root) << endl;);
    debug(cout << "  root->ref_cnt = " << root->ref_cnt << endl;);
    return version(snapshot_graph(root));
  }

  void release_version(version&& S) {
    auto root = S.graph.get_root();
    debug(cout << "== Releasing root = " << ((size_t)root) << " " << root->ref_cnt << endl;);

    assert(root->ref_cnt > 0);
    S.graph.clear_root(); //relinquish ownership

    release();
  }


  // single-entry
  void insert_edges_batch(size_t m, tuple<uintV, uintV>* edges, bool sorted=false, bool remove_dups=false, size_t nn = std::numeric_limits<size_t>::max(), bool run_seq=false) {
    debug(cout << "Insert_edges_batch" << endl;);
    size_t idx = acquire_update();
    auto root = tdata[padding*idx].version; // no ref-bump here, just a raw pointer to a tree root

    snapshot_graph G(root);

    debug(cout << "G.n = " << G.num_vertices() << endl;);
    // 1. Insert the new graph (not yet visible) into the live versions set
    snapshot_graph G_next = G.insert_edges_batch(m, edges, sorted, remove_dups, nn, run_seq);

    debug(cout << "G_next.n = " << G_next.num_vertices() << endl;);
    assert(G_next.get_root());
    auto new_root = G_next.get_root();
    debug(cout << "new_root ref_cnt = " << new_root->ref_cnt << endl;);

    set(G_next.get_root());
    debug(cout << "Finished insert edges" << endl << endl;
    cout << "n = " << G.num_vertices() << endl;
    cout << "n' = " << G_next.num_vertices() << endl;);

    G_next.clear_root();
    G.clear_root();

    release(); // it is the current version; call it a day
  }


  // single-entry
  void delete_edges_batch(size_t m, tuple<uintV, uintV>* edges, bool sorted=false, bool remove_dups=false, size_t nn = std::numeric_limits<size_t>::max(), bool run_seq=false) {
    debug(cout << "Delete_edges_batch" << endl;);
    size_t idx = acquire_update();
    debug(cout << "idx = " << idx << endl;);
    auto root = tdata[padding*idx].version;
    debug(cout << "root = " << ((size_t) root) << " " << root->ref_cnt << endl;);

    snapshot_graph G(root);
    debug(cout << "G.n = " << G.num_vertices() << endl;);

    // 1. Insert the new graph (not yet visible) into the live versions set
    snapshot_graph G_next = G.delete_edges_batch(m, edges, sorted, remove_dups, nn, run_seq);
    assert(G_next.get_root());
    auto new_root = G_next.get_root();
    debug(cout << "new_root ref_cnt = " << new_root->ref_cnt << endl;);

    set(G_next.get_root());
    debug(cout << "Finished delete edges" << endl << endl;);

    G_next.clear_root();
    G.clear_root();

    release();
  }

};
