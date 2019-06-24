#pragma once

#include "../pbbslib/utilities.h"
#include "../pbbslib/sequence_ops.h"

template <class K, class V, class H>
class sparse_table_hash {
public:
  using T = tuple<K, V>;

  size_t m;
  size_t mask;
  T empty;
  K empty_key;
  T* table;
  bool alloc;
  H hash_fn;

  static void clearA(T* A, long n, T kv) {
    parallel_for(0, n, [&] (size_t i) { A[i] = kv; });
  }

  inline size_t hashToRange(uintV h) {return h & mask;}
  inline size_t firstIndex(K& k) {return hashToRange(hash_fn(k));}
  inline size_t incrementIndex(uintV h) {return hashToRange(h+1);}

  void del() {
    if (alloc) {
      free(table);
      alloc = false;
    }
  }

  sparse_table_hash() : m(0) {
    mask = 0; alloc = false;
  }

  // Size is the maximum number of values the hash table will hold.
  // Overfilling the table could put it into an infinite loop.
  sparse_table_hash(size_t _m, T _empty, H _h) :
      m((size_t) 1 << pbbs::log2_up((size_t)(1.1*_m))),
      mask(m-1),
      empty(_empty),
      empty_key(get<0>(empty)), hash_fn(_h) {
    size_t line_size = 64;
    size_t bytes = ((m * sizeof(T))/line_size + 1)*line_size;
    table = (T*)aligned_alloc(line_size, bytes);
    clearA(table, m, empty);
    alloc=true;
  }

  // Size is the maximum number of values the hash table will hold.
  // Overfilling the table could put it into an infinite loop.
  sparse_table_hash(size_t _m, T _empty, T* _tab, H _h) :
      m(_m),
      mask(m-1),
      table(_tab),
      empty(_empty),
      empty_key(get<0>(empty)), hash_fn(_h) {
    clearA(table, m, empty);
    alloc=false;
  }

  bool insert(tuple<K, V> kv) {
    K k = get<0>(kv);
    size_t h = firstIndex(k);
    while (1) {
      if(get<0>(table[h]) == empty_key) {
        if (pbbs::atomic_compare_and_swap(&std::get<0>(table[h]),empty_key,k)) {
          std::get<1>(table[h]) = std::get<1>(kv);
          return 1;
        }
      }
      if (std::get<0>(table[h]) == k) {
        return false;
      }
      h = incrementIndex(h);
    }
    return 0;
  }

  bool insert_seq(tuple<K, V> kv) {
    K k = get<0>(kv);
    size_t h = firstIndex(k);
    while (1) {
      if(get<0>(table[h]) == empty_key) {
        table[h] = kv;
        return 1;
      }
      if (std::get<0>(table[h]) == k) {
        return false;
      }
      h = incrementIndex(h);
    }
    return 0;
  }

  void mark_seq(K k) {
    size_t h = firstIndex(k);
    while (1) {
      if(get<0>(table[h]) == empty_key) {
        return;
      }
      if (std::get<0>(table[h]) == k) {
        get<0>(table[h]) = empty_key-1;
        return;
      }
      h = incrementIndex(h);
    }
  }

  bool contains(K k) {
    size_t h = firstIndex(k);
    while (1) {
      if (get<0>(table[h]) == k) {
        return 1;
      } else if (get<0>(table[h]) == empty_key) {
        return 0;
      }
      h = incrementIndex(h);
    }
    return 0;
  }

  auto entries() {
    auto t_seq = pbbs::make_range(table, table+m);
    auto pred = [&] (T& t) { return get<0>(t) != empty_key; };
    return pbbs::filter(t_seq, pred);
  }

  void clear() {
    parallel_for(0, m, [&] (size_t i) {
      table[i] = empty;
    });
  }
};

template <class K, class V, class H>
auto make_sparse_table_hash(size_t _m, tuple<K, V> _empty, H _h) {
  return sparse_table_hash<K, V, H>(_m, _empty, _h);
}
