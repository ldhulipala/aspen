#pragma once

#include "uncompressed_iter.h"

namespace uncompressed_lists {

  using array_type = uintV;
  using uchar = unsigned char;
  using read_iter = uncompressed_iter::read_iter;

  // select a head with probability 1/2^{head_frequency}
  // expected list size is equal to 2^{head_frequency}
  static constexpr const size_t head_frequency = 6;
  static constexpr const size_t head_mask = (1 << head_frequency) - 1;

  static bool is_head(const uintV& vtx_id) {
    return (pbbs::hash64(vtx_id) & head_mask) == 0;
  }

  size_t node_size(uintV const* node) {
    if (node) {
      return node[0];
    }
    return static_cast<size_t>(0);
  }


  bool contains(uintV const* node, uintV src, uintV key) {
    if (node) {
      size_t deg = node[0];
      uintV const* edges = node + 1;
      for (size_t i=0; i<deg; i++) {
        if (edges[i] == key) {
          return true;
        }
      }
    }
    return false;
  }

  template <class F>
  inline void map_array(uintV const* node, const uintV& src, size_t offset, const F& f) {
    if (node) {
      size_t deg = node[0];
      uintV const* edges = node + 1;
      for (size_t i=0; i<deg; i++) {
        f(edges[i], offset+i);
      }
    }
  }

  template <class F>
  inline void map_nghs(uintV const* node, const uintV& src, const F& f) {
    if (node) {
      size_t deg = node[0];
      uintV const* edges = node + 1;
      for (size_t i=0; i<deg; i++) {
        f(src, edges[i]);
      }
    }
  }

  template <class P>
  inline size_t count_pred(uintV const* node, const uintV& src, const P& p) {
    size_t ct = 0;
    if (node) {
      size_t deg = node[0];
      uintV const* edges = node + 1;
      for (size_t i=0; i<deg; i++) {
        if (p(src, edges[i])) ct++;
      }
    }
    return ct;
  }

  template <class F>
  inline bool iter_elms_cond(uintV const* node, const uintV& src, const F& f) {
    if (node) {
      size_t deg = node[0];
      uintV const* edges = node + 1;
      for (size_t i=0; i<deg; i++) {
        if (f(edges[i])) return true;
      }
    }
    return false;
  }

  uintV* generate_tree_node(uintV key, uintV src, pbbs::sequence<uintV> const &S,
                            size_t start, size_t end) {
    if (start >= end) return nullptr;
    size_t deg = end - start;
    auto write_iter = uncompressed_iter::write_iter(src, deg);
    for (size_t i=start; i<end; i++) {
      uintV ngh = S[i];
      write_iter.compress_next(ngh);
    }
    write_iter.finish();
    return write_iter.node_ptr;
  }

  uintV* generate_plus(pbbs::sequence<uintV> const &S, size_t first_head_ind, uintV src) {
    if (first_head_ind == 0) return nullptr;
    // deg is first_head_ind.
    auto write_iter = uncompressed_iter::write_iter(src, first_head_ind);
    for (size_t i=0; i<first_head_ind; i++) {
      uintV ngh = S[i];
      write_iter.compress_next(ngh);
    }
    write_iter.finish();
    return write_iter.node_ptr;
  }

  inline tuple<uintV, uintV, size_t> first_and_last_keys(uintV const* node, const uintV& src) {
    uintV first_key = UINT_V_MAX;
    uintV last_key = UINT_V_MAX;
    size_t deg = 0;
    if (node) {
      deg = node[0];
      uintV const* edges = node + 1;
      first_key = edges[0]; last_key = edges[deg-1];
    }
    return make_tuple(first_key, last_key, deg);
  }

  tuple<uintV*, uintV*, bool> split_node(uintV const* node, uintV src, uintV split_key) {
    uintV* left = nullptr; uintV* right = nullptr; bool found = false;
    if (node) {
      size_t deg = node[0];
      uintV const* edges = node + 1;
      size_t left_idx = 0;
      while (left_idx < deg) {
        if (edges[left_idx] == split_key) {
          found = true; break;
        } else if (edges[left_idx] > split_key) {
          break;
        }
        left_idx++;
      }
      if (left_idx) {
        auto write_iter = uncompressed_iter::write_iter(src, left_idx);
        for (size_t i=0; i<left_idx; i++) {
          write_iter.compress_next(edges[i]);
        }
        write_iter.finish();
        left = write_iter.node_ptr;
      }
      if (found) left_idx++;
      size_t remaining = deg - left_idx;
      if (remaining) {
        auto write_iter = uncompressed_iter::write_iter(src, remaining);
        for (size_t i=left_idx; i<deg; i++) {
          write_iter.compress_next(edges[i]);
        }
        write_iter.finish();
        right = write_iter.node_ptr;
      }
    }
    return make_tuple(left, right, found);
  }

  inline uintV* copy_from_it(read_iter& it, size_t deg, uintV src) {
    auto write_iter = uncompressed_iter::write_iter(src, deg);
    for (size_t i=0; i<deg; i++) {
      write_iter.compress_next(it.next());
    }
    write_iter.finish();
    return write_iter.node_ptr;
  }

  inline uintV* copy_arr(uintV const* edges, size_t deg, uintV src) {
    auto write_iter = uncompressed_iter::write_iter(src, deg);
    for (size_t i=0; i<deg; i++) {
      write_iter.compress_next(edges[i]);
    }
    write_iter.finish();
    return write_iter.node_ptr;
  }

  uintV* copy_node(uintV const* node) {
    if (node) {
      return copy_arr(node+1, node[0], 0); // TODO get rid of src where unnecessary
    }
    return nullptr;
  }

  uintV* union_it_and_node(read_iter& it, size_t l_size, uintV const* r_node, uintV src) {
    assert(r_node);
    uintV const* r = r_node+1; size_t r_size = r_node[0];

    auto write_iter = uncompressed_iter::write_iter(src, l_size + r_size);
    size_t l_i = 0, r_i = 0;
    uintV lv = it.next(), rv = r[r_i];
    while (l_i < l_size && r_i < r_size) {
      if (lv < rv) {
        write_iter.compress_next(lv); l_i++;
        if (l_i < l_size) { lv = it.next(); }
      } else if (lv > rv) {
        write_iter.compress_next(rv); r_i++;
        rv = r[r_i];
      } else {
        write_iter.compress_next(lv); l_i++; r_i++;
        rv = r[r_i];
        if (l_i < l_size) { lv = it.next(); }
      }
    }
    while (l_i < l_size) {
      write_iter.compress_next(lv); l_i++;
      if (l_i < l_size) it.next();
    }
    for (size_t i=r_i; i<r_size; i++) {
      write_iter.compress_next(r[i]);
    }
    write_iter.finish();
    return write_iter.node_ptr;
  }


  tuple<uintV*, uintV> union_2(uintV const* l, size_t l_size, uintV const* r, size_t r_size) {
    assert(l); assert(r);
    uintV* ret = pbbs::new_array_no_init<uintV>(l_size + r_size);
    uintV ct = 0;
    size_t l_i = 0, r_i = 0;
    while (l_i < l_size && r_i < r_size) {
      uintV lv = l[l_i], rv = r[r_i];
      if (lv < rv) {
        ret[ct++] = lv; l_i++;
      } else if (lv > rv) {
        ret[ct++] = rv; r_i++;
      } else {
        ret[ct++] = lv; l_i++; r_i++;
      }
    }
    for (size_t i=l_i; i<l_size; i++) {
      ret[ct++] = l[i];
    }
    for (size_t i=r_i; i<r_size; i++) {
      ret[ct++] = r[i];
    }
    return make_tuple(ret, ct);
  }

  // r / l
  tuple<uintV*, uintV> difference_2(uintV const* l, size_t l_size, uintV const* r, size_t r_size) {
    assert(l); assert(r);
    uintV* ret = pbbs::new_array_no_init<uintV>(l_size + r_size);
    uintV ct = 0;
    size_t l_i = 0, r_i = 0;
    while (l_i < l_size && r_i < r_size) {
      uintV lv = l[l_i], rv = r[r_i];
      if (lv < rv) {
        l_i++;
      } else if (lv > rv) {
        ret[ct++] = rv; r_i++;
      } else {
        l_i++; r_i++;
      }
    }
    for (size_t i=r_i; i<r_size; i++) {
      ret[ct++] = r[i];
    }
    return make_tuple(ret, ct);
  }

  size_t intersect(uintV const* l, size_t l_size, uintV const* r, size_t r_size) {
    assert(l); assert(r);
    size_t ct = 0;
    size_t l_i = 0, r_i = 0;
    while (l_i < l_size && r_i < r_size) {
      uintV lv = l[l_i], rv = r[r_i];
      if (lv < rv) {
        l_i++;
      } else if (lv > rv) {
        r_i++;
      } else {
        ct++; l_i++; r_i++;
      }
    }
    return ct;
  }


  uintV* union_arrs(uintV const* l, size_t l_size, uintV const* r, size_t r_size, uintV src) {
    assert(l); assert(r);
    auto write_iter = uncompressed_iter::write_iter(src, l_size+r_size);
    size_t l_i = 0, r_i = 0;
    while (l_i < l_size && r_i < r_size) {
      uintV lv = l[l_i], rv = r[r_i];
      if (lv < rv) {
        write_iter.compress_next(lv); l_i++;
      } else if (lv > rv) {
        write_iter.compress_next(rv); r_i++;
      } else {
        write_iter.compress_next(lv); l_i++; r_i++;
      }
    }
    for (size_t i=l_i; i<l_size; i++) {
      write_iter.compress_next(l[i]);
    }
    for (size_t i=r_i; i<r_size; i++) {
      write_iter.compress_next(r[i]);
    }
    write_iter.finish();
    return write_iter.node_ptr;
  }

  uintV* union_nodes(uintV const* l, uintV const* r, uintV src) {
    return union_arrs(l+1, l[0], r+1, r[0], src);
  }


  template <class P>
  uintV* filter(uintV const* node, uintV src, P& p) {
    cout << "Unimplemented" << endl;
    exit(-1);
    return nullptr;
  }

  size_t intersect(uintV* l, uintV l_src, uintV* r, uintV r_src) {
    cout << "Unimplemented" << endl;
    exit(-1);
    return 0;
  }


  // returns r_node / it
  uintV* difference_it_and_node(uncompressed_iter::read_iter& it, size_t l_size, const uintV* r_node, uintV src) {
    cout << "Unimplemented" << endl;
    exit(-1);
    return nullptr;
  }

  // returns r / l
  uintV* difference(uintV const * l, uintV const* r, uintV src) {
    cout << "Unimplemented" << endl;
    exit(-1);
    return nullptr;
//    if (l && r == nullptr) return nullptr;
//    if (r && l == nullptr) return copy_node(r);
//    auto l_it = compressed_iter::read_iter(l, src);
//    auto l_size = l_it.deg;
//    auto r_it = compressed_iter::read_iter(r, src);
//    auto r_size = r_it.deg;
//    return difference_its(l_it, l_size, r_it, r_size, src);
  }


  inline size_t uncompressed_size(uintV const* node, const uintV& src) {
    size_t ct = 0;
    return ct;
  }

  void print_node(uintV const* l) {
    if (l) {
      size_t deg = l[0];
      for (size_t i=0; i<deg; i++) {
        cout << l[i+1] << " ";
      }
      cout << endl;
    } else {
      cout << "[empty node]" << endl;
    }
  }

} // namespace compression
