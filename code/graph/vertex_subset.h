#pragma once

#include <functional>
#include <limits>

using namespace std;

// no data for now, make this more full-featured later.
struct vertex_subset {
  using S = uintV;
  using D = bool;
  using VS = vertex_subset;

  // An empty vertex set.
  vertex_subset(size_t _n) : n(_n), m(0), s(nullptr), d(nullptr), is_dense(0) {}

  // A vertex set containing one element
  vertex_subset(size_t _n, uintV v) : n(_n), m(1), d(nullptr), is_dense(0) {
    s = pbbs::new_array_no_init<uintV>(1);
    s[0] = v;
  }

  // A sparse vertex set
  vertex_subset(size_t _n, size_t _m, uintV* _s) : n(_n), m(_m), s(_s),
      d(nullptr), is_dense(0) {}

  // A dense vertex set
  vertex_subset(size_t _n, size_t _m, bool* _d) : n(_n), m(_m), s(nullptr),
      d(_d), is_dense(1) {}

  // A dense vertex set
  vertex_subset(size_t _n, bool* _d) : n(_n), m(0), s(nullptr),
      d(_d), is_dense(1) {
    auto d_map = pbbs::delayed_seq<size_t>(n, [&] (size_t i) { return _d[i]; });
    m = pbbs::reduce(d_map, pbbs::addm<size_t>());
  }

  void del() {
    if (s) { pbbs::free_array(s); s = nullptr; }
    if (d) { pbbs::free_array(d); d = nullptr; }
  }

  bool is_empty() {
    return m == 0;
  }

  size_t size() {
    return m;
  }

  bool is_in(uintV v) {
    return d[v];
  }

  void to_dense() {
    if (!is_dense) {
      assert(d == nullptr);
      d = pbbs::new_array_no_init<bool>(n);
      parallel_for(0, n, [&] (size_t i) {
        d[i] = 0;
      });
      parallel_for(0, m, [&] (size_t i) {
        uintV ngh = s[i];
        d[ngh] = 1;
      });
      is_dense = true;
    }
  }

  void to_sparse() {
    if (s == nullptr && m > 0) {
      auto f_in = pbbs::delayed_seq<bool>(n, [&](size_t i) { return d[i]; });
      auto out = pbbs::pack_index<uintV>(f_in);
      s = out.to_array();
//      auto out = seq::packIndex<uintV>(d, n);
//      auto f = pbbs::sequence<bool>(d, n);
//      auto out = pbbs::pack_index<uintV>(f, n);
//      pack_t.next("pack time");
//      cout << "n = " << n << " output size = " << out.n << endl;
//      out.allocated = false;
//      s = out.A;
    }
  }

  template <class E, class M, class R>
  E map_reduce(M map_f, R red_f, E zero) {
    auto red_m = pbbs::make_monoid(red_f, zero);
    if (!is_dense) {
      auto get_val = [&] (size_t i) { return map_f(s[i]); };
      auto S = pbbs::sequence<E>(m, get_val);
      return pbbs::reduce(S, red_m);
    }
    auto get_val = [&] (size_t i) { return (d[i]) ? map_f(i) : zero; };
    auto D = pbbs::sequence<E>(n, get_val);
    return pbbs::reduce(D, red_m);
  };

  size_t n, m;
  S* s;
  D* d;
  bool is_dense;
};

template <class F>
void vertex_map(vertex_subset& vs, F f) {
  size_t m = vs.size();
  if (m > 0) {
    if (vs.is_dense) {
      size_t n = vs.n;
      parallel_for(0, n, [&] (size_t i) {
        if (vs.d[i]) {
          f(i);
        }
      });
    } else {
      parallel_for(0, m, [&] (size_t i) {
        f(vs.s[i]);
      });
    }
  }
}

// Adds vertices to a vertex_subset vs.
// Caller must ensure that every v in new_verts is not already in vs
// Note: Mutates the given vertex_subset.
void add_to_vsubset(vertex_subset& vs, uintV *new_verts,
    uintV num_new_verts) {
  if (vs.is_dense) {
    parallel_for (0, num_new_verts, [&] (size_t i) {
      vs.d[new_verts[i]] = true;
    });
    vs.m += num_new_verts;
  } else {
    const size_t vs_size = vs.size();
    const size_t new_size = num_new_verts + vs_size;
    uintV* all_verts = pbbs::new_array_no_init<uintV>(new_size);
    parallel_for (0, new_size, [&] (size_t i) {
      if (i < vs_size) {
        all_verts[i] = vs.s[i];
      } else {
        all_verts[i] = new_verts[i - vs_size];
      }
    });
    auto old_s = vs.s;
    vs.s = all_verts;
    pbbs::free_array(old_s);
    vs.m = new_size;
  }
}

