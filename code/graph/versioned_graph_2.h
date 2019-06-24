#pragma once

// Defines the API that readers and the writer use to acquire versions of the
// graph and commit new versions, making them readable.
#include "tree_plus/immutable_graph_tree_plus.h"
#include "traversible_graph.h"

#include "../lib_extensions/sequentialHT.h"
#include <limits>

namespace ut {
  static uint64_t combine(uint32_t current_ts, uint32_t ref_ct) {
    return (static_cast<uint64_t>(current_ts) << 32UL) | static_cast<uint64_t>(ref_ct);
  }
  static uint32_t get_rfct(uint64_t ts_and_ct) {
    static constexpr const uint64_t mask = std::numeric_limits<uint32_t>::max();
    return ts_and_ct & mask;
  }
  static uint32_t get_ts(uint64_t ts_and_ct) {
    static constexpr const uint64_t mask = std::numeric_limits<uint32_t>::max();
    return ts_and_ct >> 32UL;
  }
}

using ts = uint32_t;
static constexpr const ts max_ts = std::numeric_limits<uint32_t>::max();
static constexpr const ts tombstone = max_ts - 1;

static constexpr const uint64_t empty_key = std::numeric_limits<uint64_t>::max();

template <class snapshot_graph>
struct versioned_graph {

  ts current_timestamp;

  using Node = typename snapshot_graph::Node;
  using Node_GC = typename snapshot_graph::Node_GC;

  using K = uint64_t;
  using V = Node*;
  using T = tuple<K, V>;
  using table = sequentialHT<K, V>;
  table live_versions;
  T empty = make_tuple(empty_key, nullptr);

  T* latest_loc;

  struct version {
    ts timestamp;
    T* table_entry;
    snapshot_graph graph;
    version(ts _timestamp, T* _table_entry, snapshot_graph&& _graph) :
      timestamp(_timestamp), table_entry(_table_entry) {
      graph.set_root(_graph.get_root());
      _graph.clear_root();
    }
  };

  struct snapshot {
    ts timestamp;
    T* table_entry;
    Node* root;

    snapshot(ts _timestamp, T* _table_entry, Node* _root) :
      timestamp(_timestamp), table_entry(_table_entry), root(_root) { }
    snapshot(snapshot&& ss) {
      if (this != &ss) {
        this->timestamp = ss.timestamp;
        this->table_entry = ss.table_entry;
        this->root = ss.root;
        ss.timestamp = max_ts;
        ss.table_entry = nullptr;
        ss.graph = nullptr;;
      }
    }

    size_t get_ref_cnt() {
      return get<0>(get<1>(*table_entry));
    }
  };

  versioned_graph() {
    snapshot_graph::init(100000, 1000000);
    current_timestamp=0;
    size_t initial_ht_size = 512;
    live_versions = table(initial_ht_size, empty, tombstone);

    ts timestamp = current_timestamp++;
    latest_loc = live_versions.insert(make_tuple(ut::combine(timestamp, 1), nullptr));
    assert(latest_loc != nullptr);
  }

  versioned_graph(snapshot_graph&& G) {
    size_t initial_ht_size = 512;
    live_versions = table(initial_ht_size, empty, tombstone);

    ts timestamp = current_timestamp++;
    latest_loc = live_versions.insert(make_tuple(ut::combine(timestamp, 1), G.get_root()));
    assert(latest_loc != nullptr);
    G.clear_root();
  }

  versioned_graph(size_t n, size_t m, uintE* offsets, uintV* edges) : current_timestamp(0) {
    size_t initial_ht_size = 512;
    live_versions = table(initial_ht_size, empty, tombstone);

    auto G = snapshot_graph(n, m, offsets, edges);
    ts timestamp = current_timestamp++;
    latest_loc = live_versions.insert(make_tuple(ut::combine(timestamp, 1), G.get_root()));
    assert(latest_loc != nullptr);
    G.clear_root();
  }

  ts latest_timestamp() {
    return current_timestamp-1;
  }

  // Lock-free, but not wait-free
  version acquire_version() {
    auto ts_eq = [&] (uint64_t l, uint64_t r) {
      return (l >> 32UL) == (r >> 32UL);
    };
    while (true) {
      size_t latest_ts = latest_timestamp();
      tuple<T&, bool> ref_and_valid = live_versions.find(ut::combine(latest_ts, 0), ts_eq);
      T& table_ref = get<0>(ref_and_valid); bool valid = get<1>(ref_and_valid);
      if (valid) {
        while(true) {
          // can't be max_ts in a probe sequence
          if (get<0>(table_ref) == tombstone) {
            break;
          }
          uint64_t refct_and_ts = get<0>(table_ref);
          uint64_t next_value = refct_and_ts + 1;
          size_t ref_ct_before = ut::get_rfct(refct_and_ts);
          size_t ts = ut::get_ts(get<0>(table_ref));
          if (ref_ct_before > 0) {
            if (pbbs::atomic_compare_and_swap(&get<0>(table_ref), refct_and_ts, next_value)) {
              return version(ts, &table_ref, std::move(snapshot_graph(get<1>(table_ref))));
            }
          } else { // refct == 0
            break;
          }
        }
      }
    }
  }

  void release_version(version&& S) {
    ts timestamp = S.timestamp;
    T* table_entry = S.table_entry;
    auto root = S.graph.get_root();
    S.graph.clear_root(); // relinquish ownership

    uint64_t* ref_ct_loc = &(get<0>(*table_entry));
    if (ut::get_rfct(pbbs::fetch_and_add(ref_ct_loc, -1)) == 2 &&
        timestamp != latest_timestamp()) {
      // read again and try to free.
      uint64_t cur_val = *ref_ct_loc;
      if (ut::get_rfct(cur_val) == 1) {

        if (pbbs::atomic_compare_and_swap(ref_ct_loc, cur_val, cur_val-1)) {
          // no longer possible for new readers to acquire
//          if (root) { // might be an empty graph
//            assert(root->ref_cnt == 1);
//          }
          size_t ref_ct = root->ref_cnt;
          if (!ref_ct == 1) {
            cout << "issues " << root->ref_cnt << endl;
            exit(0);
          }
          bool ret = Node_GC::decrement_recursive(root); // finish it off
          assert(ret == true);

          std::get<1>(*table_entry) = nullptr;
          std::get<0>(*table_entry) = tombstone;
        }
      }
    }
  }

  // Only used by a single-threaded updater
  version acquire_latest() {
    size_t latest_ts = latest_timestamp();
    return version(latest_ts, latest_loc, std::move(snapshot_graph(get<1>(*latest_loc))));
  }

  // Only used by a single-threaded updater
  void release_latest(version&& S) {
    size_t ts = S.timestamp; // inv: earlier than latest_timestamp()
    T* table_entry = S.table_entry;
    auto root = S.graph.get_root();
    S.graph.clear_root();

    uint64_t* ref_ct_loc = &(get<0>(*table_entry));
    uint64_t ts_and_refct = *ref_ct_loc;
    if (ut::get_rfct(ts_and_refct) == 1) { // no-one is reading currently
      if (pbbs::atomic_compare_and_swap(ref_ct_loc, ts_and_refct, ts_and_refct - 1)) {
        // can collect
        size_t ref_ct = root->ref_cnt;
        if (!ref_ct == 1) {
          cout << "issues " << root->ref_cnt << endl;
          exit(0);
        }
        bool ret = Node_GC::decrement_recursive(root); // finish it off
        assert(ret == true);

        std::get<1>(*table_entry) = nullptr;
        std::get<0>(*table_entry) = tombstone;
      }
    }
  }


  // single-entry
  void insert_edges_batch(size_t m, tuple<uintV, uintV>* edges, bool sorted=false, bool remove_dups=false, size_t nn = std::numeric_limits<size_t>::max(), bool run_seq=false) {
    auto S = acquire_latest();
    const auto& G = S.graph;

    // 1. Insert the new graph (not yet visible) into the live versions set
    snapshot_graph G_next = G.insert_edges_batch(m, edges, sorted, remove_dups, nn, run_seq);
    assert(G_next.get_root());
    latest_loc = live_versions.insert(make_tuple(ut::combine(current_timestamp, 1),
                                               G_next.get_root()));
    assert(latest_loc != nullptr);
    G_next.clear_root();
    // 2. Make the new version visible
    pbbs::fetch_and_add(&current_timestamp, 1);

    release_latest(std::move(S));
  }

  // single-entry
  void delete_edges_batch(size_t m, tuple<uintV, uintV>* edges, bool sorted=false, bool remove_dups=false, size_t nn = std::numeric_limits<size_t>::max(), bool run_seq=false) {
    auto S = acquire_latest();
    const auto& G = S.graph;

    // 1. Insert the new graph (not yet visible) into the live versions set
    snapshot_graph G_next = G.delete_edges_batch(m, edges, sorted, remove_dups, nn, run_seq);
    latest_loc = live_versions.insert(make_tuple(ut::combine(current_timestamp, 1),
                                               G_next.get_root()));
    assert(latest_loc != nullptr);
    G_next.clear_root();

    // 2. Make the new version visible
    pbbs::fetch_and_add(&current_timestamp, 1);

    release_latest(std::move(S));
  }


};
