#pragma once

#include "../../common/compression.h"
//#include "compressed_iter_fat.h"
#include "compressed_iter.h"

// size_t head_frequency = 8;
// size_t head_mask = (1 << head_frequency) - 1;

namespace compressed_lists {

  using uchar = unsigned char;
  using array_type = uchar;

  using namespace compression;

  using read_iter = compressed_iter::read_iter;

  // select a head with probability 1/2^{head_frequency}
  // expected list size is equal to 2^{head_frequency}

  static constexpr const size_t head_frequency = 8;
  static constexpr const size_t head_mask = (1 << head_frequency) - 1;

  static bool is_head(const uintV& vtx_id) {
    return (pbbs::hash32_2(vtx_id) & head_mask) == 0;
  }


  template <class F>
  inline void map_array(uchar* node, const uintV& src, size_t offset, const F& f) {
    if (node) {
      auto read_iter = compressed_iter::read_iter(node, src);
      size_t deg = read_iter.deg;
      for (size_t i=0; i<deg; i++) {
        f(read_iter.next(), offset+i);
      }
    }
  }

  template <class F>
  inline void map_nghs(uchar* node, const uintV& src, const F& f) {
    if (node) {
      auto read_iter = compressed_iter::read_iter(node, src);
      size_t deg = read_iter.deg;
      for (size_t i=0; i<deg; i++) {
        f(src, read_iter.next());
      }
    }
  }

  template <class P>
  inline size_t count_pred(uchar* node, const uintV& src, const P& p) {
    size_t ct = 0;
    if (node) {
      auto read_iter = compressed_iter::read_iter(node, src);
      size_t deg = read_iter.deg;
      for (size_t i=0; i<deg; i++) {
        if (p(src, read_iter.next())) ct++;
      }
    }
    return ct;
  }

  inline size_t uncompressed_size(uchar* node, const uintV& src) {
    size_t ct = 0;
    if (node) {
      auto read_iter = compressed_iter::read_iter(node, src);
      return std::max(1 << pbbs::log2_up((read_iter.deg + 1)*4), 16);
    }
    return ct;
  }

  template <class F>
  inline bool iter_elms_cond(uchar* node, const uintV& src, const F& f) {
    if (node) {
      auto read_iter = compressed_iter::read_iter(node, src);
      size_t deg = read_iter.deg;
      for (size_t i=0; i<deg; i++) {
        if (f(read_iter.next())) return true;
      }
    }
    return false;
  }

  template <class F>
  inline void iter_elms(uchar* node, const uintV& src, const F& f) {
    if (node) {
      auto read_iter = compressed_iter::read_iter(node, src);
      size_t deg = read_iter.deg;
      for (size_t i=0; i<deg; i++) {
        f(read_iter.next());
      }
    }
  }


  template <class SQ>
  uchar* generate_tree_node(uintV key, uintV src, SQ& S,
                            size_t start, size_t end) {
    if (start >= end) return nullptr;
    size_t deg = end - start;
    auto write_iter = compressed_iter::write_iter(src, deg);
    for (size_t i=start; i<end; i++) {
      uintV ngh = S[i];
      write_iter.compress_next(ngh);
    }
    return write_iter.finish();
  }

  template <class SQ>
  uchar* generate_plus(SQ& S, size_t first_head_ind, uintV src) {
    if (first_head_ind == 0) return nullptr;
    auto write_iter = compressed_iter::write_iter(src, first_head_ind);
    for (size_t i=0; i<first_head_ind; i++) {
      uintV ngh = S[i];
      write_iter.compress_next(ngh);
    }
    return write_iter.finish();
  }

  inline tuple<uintV, uintV> first_and_last_keys(uchar* node, const uintV& src) {
    uintV first_key = UINT_V_MAX; uintV last_key = UINT_V_MAX;
    if (node) {
      auto read_iter = compressed_iter::read_iter(node, src);
      first_key = read_iter.next();
      last_key = read_iter.last_key();
    }
    return make_tuple(first_key, last_key);
  }

  void print_node(uchar* node, uintV src) {
    if (node) {
      auto read_iter = compressed_iter::read_iter(node, src);
      while (read_iter.has_next()) {
        cout << read_iter.next() << endl;
      }
    }
  }

  auto split_node(uchar* node, uintV src, uintV split_key) {
    uchar* lt = nullptr; uchar* gt = nullptr; bool found = false;
    if (node) {
      auto read_iter = compressed_iter::read_iter(node, src);
      size_t deg = read_iter.deg;
      size_t num_lt=0;

      while (num_lt < deg) {
        uintV ngh = read_iter.next();
        if (ngh == split_key) { found = true; break; }
        if (ngh > split_key) { break; }
        num_lt++;
      }
      size_t num_gt = deg - num_lt - static_cast<size_t>(found);

      read_iter = compressed_iter::read_iter(node, src);
      if (num_lt) {
        auto lt_iter = compressed_iter::write_iter(src, num_lt);
        for (size_t i=0; i<num_lt; i++)
          lt_iter.compress_next(read_iter.next());
        lt = lt_iter.finish();
      }
      if (found) read_iter.next();
      if (num_gt) {
        auto gt_iter = compressed_iter::write_iter(src, num_gt);
        for (size_t i=0; i<num_gt; i++)
          gt_iter.compress_next(read_iter.next());
        gt = gt_iter.finish();
      }
    }
    return make_tuple(lt, gt, found);
  }


  inline uchar* copy_from_it(compressed_iter::read_iter& it, size_t deg, uintV src) {
    auto write_iter = compressed_iter::write_iter(src, deg);
    for (size_t i=0; i<deg; i++) {
      write_iter.compress_next(it.next());
    }
    return write_iter.finish();
  }

  uchar* copy_node(uchar* node) {
    if (node) {
      // bump ref_ct and return
      uint32_t* ref_ct = (uint32_t*)(node + 2*sizeof(uint16_t));
      pbbs::write_add(ref_ct, 1);
      return node;
//      size_t total_bytes = *((uint16_t*)(node + sizeof(uint16_t)));
//      auto T = alloc_node(total_bytes);
//      uchar* new_node = get<0>(T);
//      std::memcpy(new_node, node, total_bytes);
//      return new_node;
    }
    return nullptr;
  }

  uchar* union_its(compressed_iter::read_iter& l_it, size_t l_size,
                   compressed_iter::read_iter& r_it, size_t r_size,
                   uintV src) {
    auto write_iter = compressed_iter::write_iter(src, l_size + r_size);
    size_t l_i = 0, r_i = 0;
    uintV lv = l_it.next(), rv = r_it.next();;
    while (l_i < l_size && r_i < r_size) {
      if (lv < rv) {
        write_iter.compress_next(lv); l_i++;
        if (l_i < l_size) { lv = l_it.next(); }
      } else if (lv > rv) {
        write_iter.compress_next(rv); r_i++;
        if (r_i < r_size) { rv = r_it.next(); }
      } else {
        write_iter.compress_next(lv); l_i++; r_i++;
        if (l_i < l_size) { lv = l_it.next(); }
        if (r_i < r_size) { rv = r_it.next(); }
      }
    }
    while (l_i < l_size) {
      write_iter.compress_next(lv); l_i++;
      if (l_i < l_size) lv = l_it.next();
    }
    while (r_i < r_size) {
      write_iter.compress_next(rv); r_i++;
      if (r_i < r_size) { rv = r_it.next(); }
    }
    return write_iter.finish();
  }


  uchar* union_it_and_node(compressed_iter::read_iter& it, size_t l_size, uchar* r_node, uintV src) {
    assert(r_node);
    auto r_it = compressed_iter::read_iter(r_node, src);
    size_t r_size = r_it.deg;
    return union_its(it, l_size, r_it, r_size, src);
  }

  uchar* union_nodes(uchar* l, uchar* r, uintV src) {
    if (l && r == nullptr) return copy_node(l);
    if (r && l == nullptr) return copy_node(r);
    auto l_it = compressed_iter::read_iter(l, src);
    auto l_size = l_it.deg;
    auto r_it = compressed_iter::read_iter(r, src);
    auto r_size = r_it.deg;
    return union_its(l_it, l_size, r_it, r_size, src);
  }

  // returns r_it / l_it
  uchar* difference_its(compressed_iter::read_iter& l_it, size_t l_size,
                        compressed_iter::read_iter& r_it, size_t r_size,
                        uintV src) {
    assert(l_size > 0); assert(r_size > 0);
    auto write_iter = compressed_iter::write_iter(src, r_size);
    size_t l_i = 0, r_i = 0;
    uintV lv = l_it.next(), rv = r_it.next();
    while (l_i < l_size && r_i < r_size) {
      if (lv < rv) {
        l_i++;
        if (l_i < l_size) { lv = l_it.next(); }
      } else if (lv > rv) {
        write_iter.compress_next(rv); r_i++;
        if (r_i < r_size) { rv = r_it.next(); }
      } else { // intersect, remove rv
        l_i++; r_i++;
        if (l_i < l_size) { lv = l_it.next(); }
        if (r_i < r_size) { rv = r_it.next(); }
      }
    }
    while (r_i < r_size) {
      write_iter.compress_next(rv); r_i++;
      if (r_i < r_size) { rv = r_it.next(); }
    }
    return write_iter.finish();
  }

  // returns r_node / it
  uchar* difference_it_and_node(compressed_iter::read_iter& it, size_t l_size, uchar* r_node, uintV src) {
    assert(r_node);
    auto r_it = compressed_iter::read_iter(r_node, src);
    size_t r_size = r_it.deg;
    return difference_its(it, l_size, r_it, r_size, src);
  }

  // returns r / l
  uchar* difference(uchar* l, uchar* r, uintV src) {
    if (l && r == nullptr) return nullptr;
    if (r && l == nullptr) return copy_node(r);
    auto l_it = compressed_iter::read_iter(l, src);
    auto l_size = l_it.deg;
    auto r_it = compressed_iter::read_iter(r, src);
    auto r_size = r_it.deg;
    return difference_its(l_it, l_size, r_it, r_size, src);
  }

  bool contains(uchar* l, uintV src, uintV key) {
    if (l) {
      auto it = compressed_iter::read_iter(l, src);
      while (it.has_next()) {
        uintV ngh = it.next();
        if (ngh == key) return true;
        if (ngh > key) return false;
      }
    }
    return false;
  }

  size_t intersect(compressed_iter::read_iter& l_it, size_t l_size,
                   compressed_iter::read_iter& r_it, size_t r_size) {
    size_t l_i = 0, r_i = 0;
    uintV lv = l_it.next(), rv = r_it.next();;
    size_t ct = 0;
    while (l_i < l_size && r_i < r_size) {
      if (lv < rv) {
        l_i++;
        if (l_i < l_size) { lv = l_it.next(); }
      } else if (lv > rv) {
        r_i++;
        if (r_i < r_size) { rv = r_it.next(); }
      } else {
        ct++; l_i++; r_i++;
        if (l_i < l_size) { lv = l_it.next(); }
        if (r_i < r_size) { rv = r_it.next(); }
      }
    }
    return ct;
  }

  size_t intersect(uchar* l, uintV l_src, uchar* r, uintV r_src) {
    if (l && r) {
      auto l_it = compressed_iter::read_iter(l, l_src);
      auto r_it = compressed_iter::read_iter(r, r_src);
      return intersect(l_it, l_it.deg, r_it, r_it.deg);
    }
    return static_cast<size_t>(0);
  }

  template <class P>
  uchar* filter(uchar* node, uintV src, P& p) {
    if (!node) return nullptr;
    auto it = compressed_iter::read_iter(node, src);
    auto write_iter = compressed_iter::write_iter(src, it.deg);
    for (size_t i=0; i<it.deg; i++) {
      uintV ngh = it.next();
      if (p(ngh)) write_iter.compress_next(ngh);
    }
    return write_iter.finish();
  }


} // namespace compression
