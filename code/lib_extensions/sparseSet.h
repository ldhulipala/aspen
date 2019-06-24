// This code is based on the paper "Phase-Concurrent Hash Tables for
// Determinism" by Julian Shun and Guy Blelloch from SPAA 2014.
// Copyright (c) 2014 Julian Shun and Guy Blelloch
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights (to use, copy,
// modify, merge, publish, distribute, sublicense, and/or sell copies
// of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
// BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
// ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
#pragma once

#include "../pbbslib/utilities.h"

using namespace std;

template <class K, class V>
class sparseAdditiveSet {
 public:
  using T = tuple<K, V>;
  size_t m;
  size_t mask;
  T empty;
  K empty_key;
  T* table;
  bool alloc;
  float load_factor;

  // needs to be in separate routine due to Cilk bugs
  static void clearA(T* A, long n, T kv) {
    parallel_for(0, n, [&] (size_t i) { A[i] = kv; });
  }

  inline size_t hashToRange(size_t h) {return h & mask;}
  inline size_t firstIndex(K& k) {return hashToRange(pbbs::hash64(k));}
  inline size_t incrementIndex(size_t h) {return hashToRange(h+1);}

  // Size is the maximum number of values the hash table will hold.
  // Overfilling the table could put it into an infinite loop.
  sparseAdditiveSet(size_t _m, T _empty, float _load_factor=1.1) :
      m((size_t) 1 << pbbs::log2_up((size_t)(_load_factor*_m))),
      mask(m-1),
      empty(_empty),
      empty_key(get<0>(empty)),
      load_factor(_load_factor) {
    size_t line_size = 64;
    size_t bytes = ((m * sizeof(T))/line_size + 1)*line_size;
    table = (T*)aligned_alloc(line_size, bytes);
    clearA(table, m, empty);
    alloc=true;
  }

  // Deletes the allocated arrays
  void del() {
    if (alloc) {
      free(table);
      alloc = false;
    }
  }

  // nondeterministic insert
  bool insert(T kv) {
    K key = get<0>(kv);
    size_t h = firstIndex(key);
    while (1) {
      if(get<0>(table[h]) == empty_key &&
         pbbs::atomic_compare_and_swap(&(table[h]),empty,kv)) {
      	return 1; //return true if value originally didn't exist
      }
      else if (get<0>(table[h]) == key) {
      	//add residual values on duplicate
        V* addr = &(get<1>(table[h]));
        V val = get<1>(kv);
        pbbs::write_add(addr,val);
      	return 0;
      }
      h = incrementIndex(h);
    }
    return 0;
  }

  T find(K v) {
    size_t h = firstIndex(v);
    T c = table[h];
    while (1) {
      if (get<0>(c) == empty_key) return empty;
      else if (get<0>(c) == v)
      	return c;
      h = incrementIndex(h);
      c = table[h];
    }
  }

  template <class F>
  void map(F f) {
    parallel_for(0, m, [&] (size_t i) {
      if(get<0>(table[i]) != empty_key) f(table[i]);
    });
  }

  template <class F>
  void mapIndex(F f) {
    parallel_for(0, m, [&] (size_t i) {
      if(get<0>(table[i]) != empty_key) f(table[i],i);});
  }

  // returns all the current entries compacted into a sequence
  auto entries() {
    auto t_seq = pbbs::make_range(table, table+m);
    auto pred = [&] (T& t) { return get<0>(t) != empty_key; };
    return pbbs::filter(t_seq, pred);
  }

  // returns all the current entries satisfying predicate f compacted into a sequence
  template <class F>
  auto entries(F f) {
    auto t_seq = pbbs::make_range(table, table+m);
    auto pred = [&] (T& t) { return get<0>(t) != empty_key && f(t); };
    return pbbs::filter(t_seq, pred);
  }

  // returns the number of entries
  size_t count() {
    auto get_non_empty = [&] (size_t i) {
      return (get<0>(table[i]) == empty_key) ?
        static_cast<size_t>(1) : static_cast<size_t>(0); };
    auto S = pbbs::delayed_seq<size_t>(m, get_non_empty);
    return pbbs::reduce(S, pbbs::addm<size_t>());
  }
};
