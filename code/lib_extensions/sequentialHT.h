#pragma once

#include "../pbbslib/utilities.h"

using namespace std;

template <class K, class V>
class sequentialHT {
 public:
  typedef tuple<K,V> T;

  size_t m, n;
  size_t mask;
  T empty;
  K max_key;
  K tombstone;
  T* table;

  inline size_t toRange(size_t h) {return h & mask;}
  inline size_t firstIndex(K v) {return toRange(pbbs::hash32(v >> 32UL));}
  inline size_t incrementIndex(size_t h) {return toRange(h+1);}

  // m must be a power of two. assumes the table is already cleared.
  sequentialHT(T* _table, size_t _m, tuple<K, V> _empty, K _tombstone) :
    m((size_t)_m), mask(m-1), empty(_empty), tombstone(_tombstone), table(_table)
  { max_key = get<0>(empty); }

  // m must be a power of two
  sequentialHT(size_t _m, tuple<K, V> _empty, K _tombstone) :
    m((size_t)_m), mask(m-1), empty(_empty), tombstone(_tombstone)
  {
    max_key = get<0>(empty);
    table = pbbs::new_array<T>(_m);
    parallel_for(0, m, [&] (size_t i) {
      table[i] = empty;
    });
  }

  sequentialHT() {}

  inline T* insert(tuple<K, V> v) {
    K key = get<0>(v);
    size_t h = firstIndex(key);
    while (1) {
      auto k = get<0>(table[h]);
      if (k == max_key || k == tombstone) {
        table[h] = v;
        n++;
        return (table + h);
      } else if (k == key) {
        return nullptr;
      }
      h = incrementIndex(h);
    }
  }

  template <class F>
  inline bool remove(K key, F deletion_fn) {
    size_t h = firstIndex(key);
    while (1) {
      auto k = get<0>(table[h]);
      if (k == max_key) {
        return false;
      } else if (k == key) {
        // TODO(laxmand): figure out what needs to be done here.
        deletion_fn(table[h]);
        get<0>(table[h]) = tombstone;
      }
      h = incrementIndex(h);
    }
  }

  inline tuple<T&,bool> find(K key) {
    size_t h = firstIndex(key);
    while (1) {
      auto& table_ref = table[h];
      if (get<0>(table_ref) == max_key) {
        return std::forward_as_tuple(table_ref, false);
      } else if (get<0>(table_ref) == key) {
      	return std::forward_as_tuple(table_ref, true);
      }
      h = incrementIndex(h);
    }
  }

  template <class Eq>
  inline tuple<T&,bool> find(K key, Eq& eq) {
    size_t h = firstIndex(key);
    while (1) {
      auto& table_ref = table[h];
      if (eq(get<0>(table_ref), max_key)) {
        return std::forward_as_tuple(table_ref, false);
      } else if (eq(get<0>(table_ref), key)) {
      	return std::forward_as_tuple(table_ref, true);
      }
      h = incrementIndex(h);
    }
  }

};

