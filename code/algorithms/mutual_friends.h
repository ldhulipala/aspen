#pragma once

#include "../lib_extensions/sparse_table.h"

template <class Graph>
size_t mutual_friends(Graph& G, uintV a, uintV b) {
//  cout << "Intersecting " << a << " " << b << endl;
  auto ma = G.find_vertex(a);
  auto mb = G.find_vertex(b);
  if (!ma.valid || !mb.valid) {
    return static_cast<size_t>(0);
  }
  auto ta = ma.value; auto tb = mb.value;
  size_t ct = tree_plus::intersect(ta, a, tb, b);
//  cout << "Finished " << a << " " << b << endl;
  return ct;
//  return ma.value.intersect(mb.value);
}
