#include "utilities.h"
#include "get_time.h"
#include "random.h"
#include "counting_sort.h"
#include "random_shuffle.h"
#include "histogram.h"
#include "integer_sort.h"
#include "sample_sort.h"
#include "merge.h"
#include "merge_sort.h"
#include "sparse_mat_vec_mult.h"
#include "sequence_ops.h"

#include <iostream>
#include <ctype.h>
#include <math.h>

static timer bt;
using namespace std;
using uchar = unsigned char;

#define time(_var,_body)    \
  bt.start();               \
  _body;		    \
  double _var = bt.stop();

template<typename T>
double t_tabulate(size_t n) {
  sequence<T> S(n, (T) 0);
  time(t, parallel_for(size_t i = 0; i < n; i++) S[i] = i;);
  return t;
}

template<typename T>
double t_map(size_t n) {
  sequence<T> In(n, (T) 1);
  sequence<T> Out(n, (T) 0);
  time(t, parallel_for(size_t i = 0; i < n; i++) Out[i] = In[i];);
  return t;
}

template<typename T>
double t_reduce_add(size_t n) {
  sequence<T> S(n, (T) 1);
  time(t, pbbs::reduce_add(S););
  return t;
}

template<typename T>
double t_scan_add(size_t n) {
  sequence<T> In(n, (T) 1);
  sequence<T> Out(n, (T) 0);
  time(t, pbbs::scan_add(In,Out););
  return t;
}

template<typename T>
double t_pack(size_t n) {
  sequence<bool> flags(n, [] (size_t i) -> bool {return i%2;});
  sequence<T> In(n, [] (size_t i) -> T {return i;});
  time(t, pbbs::pack(In, flags););
  return t;
}

template<typename T>
double t_split3_old(size_t n) {
  sequence<uchar> flags(n, [] (size_t i) -> uchar {return i%3;});
  sequence<T> In(n, [] (size_t i) -> T {return i;});
  sequence<T> Out(n, (T) 0);
  time(t, pbbs::split_three(In, Out, flags););
  return t;
}

template<typename T>
double t_split3(size_t n) {
  pbbs::random r(0);
  sequence<T> In(n, [&] (size_t i) {return r.ith_rand(i);});
  sequence<T> Out(n, (T) 0);
  time(t, pbbs::p_split3(In, Out, std::less<T>()););
  return t;
}

template<typename T>
double t_gather(size_t n) {
  pbbs::random r(0);
  sequence<T> in(n, [&] (size_t i) {return i;});
  sequence<T> out(n, (T) 0);
  sequence<T> idx(n, [&] (size_t i) {return r.ith_rand(i)%n;});
  time(t, parallel_for(size_t i=0; i<n; i++) {
      out[i] = in[idx[i]];});
  return t;
}

template<typename T>
double t_scatter(size_t n) {
  pbbs::random r(0);
  sequence<T> out(n, (T) 0);
  sequence<T> idx(n, [&] (size_t i) {return r.ith_rand(i)%n;});
  time(t, parallel_for(size_t i=0; i<n-3; i++) {
      out[idx[i]] = i;});
  return t;
}

template<typename T>
double t_write_add(size_t n) {
  pbbs::random r(0);
  sequence<T> out(n, (T) 0);
  sequence<T> idx(n, [&] (size_t i) {return r.ith_rand(i)%n;});
  time(t, parallel_for(size_t i=0; i<n-3; i++) {
      // prefetching useful on AMD, but not intel machines
      //__builtin_prefetch (&out[idx[i+3]], 1, 1);
      pbbs::write_add(&out[idx[i]],1);});
  return t;
}

template<typename T>
double t_write_min(size_t n) {
  pbbs::random r(0);
  sequence<T> out(n, (T) n);
  sequence<T> idx(n, [&] (size_t i) {return r.ith_rand(i)%n;});
  time(t, parallel_for(size_t i=0; i<n-3; i++) {
      //__builtin_prefetch (&out[idx[i+3]], 1, 1);
      pbbs::write_min(&out[idx[i]], (T) i, pbbs::less<T>());});
  return t;
}

template<typename T>
double t_shuffle(size_t n) {
  sequence<T> in(n, [&] (size_t i) {return i;});
  time(t, pbbs::random_shuffle(in,n););
  return t;
}

template<typename T>
double t_histogram(size_t n) {
  pbbs::random r(0);
  sequence<T> in(n, [&] (size_t i) {return r.ith_rand(i)%n;});
  sequence<T> out;
  time(t, out = pbbs::histogram<T>(in,n););
  return t;
}

template<typename T>
double t_histogram_few(size_t n) {
  pbbs::random r(0);
  sequence<T> in(n, [&] (size_t i) {return r.ith_rand(i)%256;});
  sequence<T> out;
  time(t, out = pbbs::histogram<T>(in,256););
  return t;
}

template<typename T>
double t_histogram_same(size_t n) {
  sequence<T> in(n, (T) 10311);
  sequence<T> out;
  time(t, out = pbbs::histogram<T>(in,n););
  return t;
}

template<typename T>
double t_sort(size_t n) {
  pbbs::random r(0);
  sequence<T> in(n, [&] (size_t i) {return r.ith_rand(i)%n;});
  sequence<T> out;
  time(t, out = pbbs::sample_sort(in, std::less<T>()););
  //for (size_t i = 1; i < n; i++)
  //  if (std::less<T>()(out[i],out[i-1])) {cout << i << endl; abort();}
  return t;
}

template<typename T>
double t_merge_sort(size_t n) {
  pbbs::random r(0);
  sequence<T> in(n, [&] (size_t i) {return r.ith_rand(i)%n;});
  sequence<T> out(n);
  time(t, pbbs::merge_sort(out, in, std::less<T>()););
  //for (size_t i = 1; i < n; i++)
  //  if (std::less<T>()(in[i],in[i-1])) {cout << i << endl; abort();}
  return t;
}

template<typename T>
double t_quicksort(size_t n) {
  pbbs::random r(0);
  sequence<T> in(n, [&] (size_t i) {return r.ith_rand(i)%n;});
  sequence<T> out(n);
  time(t, pbbs::p_quicksort(in, out, std::less<T>()););
  //for (size_t i = 1; i < n; i++)
  //  if (std::less<T>()(in[i],in[i-1])) {cout << i << endl; abort();}
  return t;
}

template<typename T>
double t_count_sort_2(size_t n) {
  pbbs::random r(0);
  size_t num_buckets = (1<<2);
  size_t mask = num_buckets - 1;
  sequence<T> in(n, [&] (size_t i) {return r.ith_rand(i);});
  auto f = [&] (size_t i) {return in[i] & mask;};
  auto keys = make_sequence<unsigned char>(n, f);

  time(t, pbbs::count_sort(in, in, keys, num_buckets););
  return t;
}

template<typename T>
double t_count_sort_8(size_t n) {
  pbbs::random r(0);
  size_t num_buckets = (1<<8);
  size_t mask = num_buckets - 1;
  sequence<T> in(n, [&] (size_t i) {return r.ith_rand(i);});
  auto f = [&] (size_t i) {return in[i] & mask;};
  auto keys = make_sequence<unsigned char>(n, f);
  time(t, pbbs::count_sort(in, in, keys, num_buckets););
  return t;
}

template<typename T>
double t_integer_sort_pair(size_t n) {
  using par = pair<T,T>;
  pbbs::random r(0);
  size_t bits = sizeof(T)*8;
  sequence<par> S(n, [&] (size_t i) -> par {
      return par(r.ith_rand(i),i);});
  auto first = [] (par a) {return a.first;};
  time(t, pbbs::integer_sort<T>(S,S,first,bits););
  return t;
}

template<typename T>
double t_integer_sort(size_t n) {
  pbbs::random r(0);
  size_t bits = sizeof(T)*8;
  sequence<T> S(n, [&] (size_t i) -> T {
      return r.ith_rand(i);});
  auto identity = [] (T a) {return a;};
  time(t, pbbs::integer_sort<T>(S,S,identity,bits););
  return t;
}

typedef unsigned __int128 long_int;
double t_integer_sort_128(size_t n) {
  pbbs::random r(0);
  size_t bits = 128;
  sequence<long_int> S(n, [&] (size_t i) -> long_int {
      return r.ith_rand(2*i) + (((long_int) r.ith_rand(2*i+1)) << 64) ;});
  auto identity = [] (long_int a) {return a;};
  time(t, pbbs::integer_sort<long_int>(S,S,identity,bits););
  return t;
}

template<typename T>
double t_merge(size_t n) {
  sequence<T> in1(n/2, [&] (size_t i) {return 2*i;});
  sequence<T> in2(n-n/2, [&] (size_t i) {return 2*i+1;});
  sequence<T> out(n, (T) 0);
  time(t, pbbs::merge(in1, in2, out, std::less<T>()););
  return t;
}

template<typename s_size_t, typename T>
double t_mat_vec_mult(size_t n) {
  pbbs::random r(0);
  size_t degree = 5;
  size_t m = degree*n;
  sequence<s_size_t> starts(n+1, [&] (size_t i) {
      return degree*i;});
  sequence<s_size_t> columns(m, [&] (size_t i) {
      return r.ith_rand(i)%n;});
  sequence<T> values(m, (T) 1);
  sequence<T> in(n, (T) 1);
  sequence<T> out(n, (T) 0);
  auto add = [] (T a, T b) { return a + b;};
  auto mult = [] (T a, T b) { return a * b;};

  time(t, mat_vec_mult(starts, columns, values, in, out, mult, add););
  return t;
}
