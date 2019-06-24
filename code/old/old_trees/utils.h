#pragma once
#include "../pbbslib/sequence_ops.h"
#include "../pbbslib/sample_sort.h"

// *******************************************
//   Utils
// *******************************************
bool tree_parallel = true;

namespace utils {
  // for granularity control
  // if the size of a node is smaller than this number then it
  // processed sequentially instead of in parallel
  constexpr const size_t node_limit = 100;


  // for two input sizes of n and m, should we do a parallel fork
  // assumes work proportional to m log (n/m + 1)
  static bool do_parallel(size_t n, size_t m) {
    if (m > n) std::swap(n,m);
    return (m > 8 && (m * pbbs::log2_up(n/m + 1)) > 100);
  }

  // fork-join parallel call, returning a pair of values
  template <class RT, class Lf, class Rf>
  static std::pair<RT,RT> fork(bool do_parallel, Lf left, Rf right) {
    if (do_parallel && tree_parallel) {
      RT r;
      auto do_right = [&] () {r = right();};
      cilk_spawn do_right();
      RT l = left();
      cilk_sync;
      return std::pair<RT,RT>(l,r);
    } else {
      RT l = left();
      RT r = right();
      return std::make_pair(l,r);
    }
  }

  // fork-join parallel call, returning nothing
  template <class Lf, class Rf>
  static void fork_no_result(bool do_parallel, Lf left, Rf right) {
    if (do_parallel && tree_parallel) {
      cilk_spawn right(); left(); cilk_sync; }
    else {left(); right();}
  }

  // fork-join parallel call, returning a pair of values
  template <class RT, class Lf, class Rf>
  static std::pair<RT,RT> fork_app(bool do_parallel, Lf left, Rf right) {
    if (do_parallel) {
      RT r;
      auto do_right = [&] () {r = right();};
      cilk_spawn do_right();
      RT l = left();
      cilk_sync;
      return std::pair<RT,RT>(l,r);
    } else {
      RT l = left();
      RT r = right();
      return std::make_pair(l,r);
    }
  }

  // fork-join parallel call, returning nothing
  template <class Lf, class Rf>
  static void fork_app_no_result(bool do_parallel, Lf left, Rf right) {
    if (do_parallel) {
      cilk_spawn right(); left(); cilk_sync; }
    else {left(); right();}
  }

  template<class V>
  struct get_left {
    V operator () (const V& a, const V& b) const
    { return a; }
  };

  template<class V>
  struct get_right {
    V operator () (const V& a, const V& b) const
    { return b; }
  };

}
