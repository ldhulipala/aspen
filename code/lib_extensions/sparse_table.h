#pragma once

#include "../pbbslib/utilities.h"
#include "../pbbslib/sequence_ops.h"

template <class K, class V>
class sparse_table {
public:
  using T = tuple<K, V>;

  size_t m;
  size_t mask;
  T empty;
  K empty_key;
  T* table;
  bool alloc;
  size_t ctr;

  static void clearA(T* A, long n, T kv) {
    parallel_for(0, n, [&] (size_t i) { A[i] = kv; });
  }

  inline size_t hashToRange(uintV h) {return h & mask;}
  inline size_t firstIndex(K& k) {return hashToRange(pbbs::hash64(k));}
  inline size_t incrementIndex(uintV h) {return hashToRange(h+1);}

  void del() {
    if (alloc) {
      pbbs::free_array(table);
      alloc = false;
    }
  }

  sparse_table() : m(0) {
    mask = 0; alloc = false;
    ctr = 0;
  }

  // Size is the maximum number of values the hash table will hold.
  // Overfilling the table could put it into an infinite loop.
  sparse_table(size_t _m, T _empty) :
      m((size_t) 1 << pbbs::log2_up((size_t)(1.1*_m))),
      mask(m-1),
      empty(_empty),
      empty_key(get<0>(empty)) {
    table = pbbs::new_array_no_init<T>(m);
//    size_t line_size = 64;
//    size_t bytes = ((m * sizeof(T))/line_size + 1)*line_size;
//    table = (T*)aligned_alloc(line_size, bytes);
    clearA(table, m, empty);
    alloc=true;
    ctr = 0;
  }

  // Size is the maximum number of values the hash table will hold.
  // Overfilling the table could put it into an infinite loop.
  sparse_table(size_t _m, T _empty, T* _tab) :
      m(_m),
      mask(m-1),
      table(_tab),
      empty(_empty),
      empty_key(get<0>(empty)) {
    clearA(table, m, empty);
    alloc=false;
    ctr = 0;
  }

  sparse_table(const sparse_table<K, V>& other) {
    this->m = other.m;
    this->mask = other.mask;
    this->table = other.table;
    this->empty = other.empty;
    this->empty_key = other.empty_key;
    ctr = 0;
  }

  // seq only
  void resize_if_necessary(size_t incoming) {
    if (ctr + incoming > (3*m/4)) {
      size_t old_m = m;
      size_t new_m = std::max(1UL << (pbbs::log2_up(old_m + incoming) + 1), 4*m);
//      size_t new_m = 2*m;
      auto new_table = pbbs::new_array_no_init<T>(new_m);
//      cout << "resizing from old_m = " << old_m << " to new_m = " << new_m << endl;
      for (size_t i=0; i<new_m; i++) {
        new_table[i] = empty;
      }
      auto old_ctr = ctr;
      ctr = 0;
      auto old_table = table;
      table = new_table;
      m = new_m;
      mask = m-1;
      for (size_t i=0; i<old_m; i++) {
        if (std::get<0>(old_table[i]) != empty_key) {
          insert_seq(old_table[i]);
        }
      }
      pbbs::free_array(old_table);
    }
  }

  // par
  void resize_if_necessary_par(size_t incoming) {
    if (ctr + incoming > (3*m/4)) {
      size_t old_m = m;
      size_t new_m = std::max(1UL << (pbbs::log2_up(old_m + incoming) + 1), 4*m);
//      size_t new_m = 2*m;
      auto new_table = pbbs::new_array_no_init<T>(new_m);
//      cout << "resizing from old_m = " << old_m << " to new_m = " << new_m << endl;
      parallel_for(0, new_m, [&] (size_t i) {
        new_table[i] = empty;
      });
      auto old_ctr = ctr;
      ctr = 0;
      auto old_table = table;
      table = new_table;
      m = new_m;
      mask = m-1;
      parallel_for(0, old_m, [&] (size_t i) {
        if (std::get<0>(old_table[i]) != empty_key) {
          insert(old_table[i]);
        }
      });
      pbbs::free_array(old_table);
    }
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
        ctr++;
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
