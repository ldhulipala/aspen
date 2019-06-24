#pragma once

#include "utilities.h"

template <typename E>
struct sequence {
public:
  using T = E;

  sequence() {}

  // copy constructor
  sequence(sequence& a) : s(a.s), e(a.e), allocated(false) {}

  // move constructor
  sequence(sequence&& b)
    : s(b.s), e(b.e), allocated(b.allocated) {
    b.s = b.e = NULL; b.allocated = false;}

  sequence(const size_t n)
    : s(pbbs::new_array<E>(n)), allocated(true) {
    e = s + n;
  };

  sequence(const size_t n, T v)
    : s(pbbs::new_array_no_init<E>(n,1)), allocated(true) {
    e = s + n;
    parallel_for (size_t i = 0; i < n; i++)
      new ((void*) (s+i)) T(v);
  };

  template <typename Func>
  sequence(const size_t n, Func fun, bool run_seq=false)
    : s(pbbs::new_array_no_init<E>(n)), allocated(true) {
    e = s + n;
    granular_for(i, 0, n, !run_seq, {
      T x = fun(i);
      new ((void*) (s+i)) T(x);
    });
  }

  sequence(E* s, const size_t n, bool allocated = false)
    : s(s), e(s + n), allocated(allocated) {};

  sequence(E* s, E* e, bool allocated = false)
    : s(s), e(e), allocated(allocated) {};

  ~sequence() { clear();}

  template <typename X, typename F>
  static sequence<X> tabulate(size_t n, F f) {
    X* r = pbbs::new_array_no_init<X>(n);
    parallel_for (size_t i = 0; i < n; i++)
      new ((void*) (r+i)) X(f(i));
    sequence<X> y(r,n);
    y.allocated = true;
    return y;
  }

  sequence copy(sequence& a) {
    return tabulate(e-s, [&] (size_t i) {return a[i];});
  }

  // copy assignment
  sequence& operator = (const sequence& m) {
    if (this != &m) {
      clear(); s = m.s; e = m.e; allocated = false;
    }
    return *this;
  }

  sequence& operator = (sequence&& m) {
    if (this != &m) {
      s = m.s; e = m.e; allocated = m.allocated;
      m.s = NULL; m.e = NULL; m.allocated=false;}
    return *this;
  }

  E& operator[] (const size_t i) const {return s[i];}

  sequence slice(size_t ss, size_t ee) {
    return sequence(s + ss, s + ee);
  }

  void update(const size_t i, T& v) {
    s[i] = v;
  }

  size_t size() const { return e - s;}

  sequence as_sequence() {
    return sequence(s, e);
  }

  T* get_array() {return s; allocated=false; }
  T* as_array() {return s;}
  T* start() {return s;}
  T* end() {return e;}

  //private:
  void clear() {
    if (allocated) {
      pbbs::delete_array<E>(s,e-s);
    }
    s = e = NULL;
    allocated=false;
  }
  E *s; // = NULL;
  E *e; // = NULL;
  bool allocated = false;
};

template <typename E, typename F>
struct func_sequence {
  using T = E;
  func_sequence(size_t n, F& _f) : f(&_f), s(0), e(n) {};
  func_sequence(size_t s, size_t e, F& _f) : f(&_f), s(s), e(e) {};
  T operator[] (const size_t i) {return (*f)(i+s);}
  func_sequence<T,F> slice(size_t ss, size_t ee) {
    return func_sequence<T,F>(s+ss,s+ee,*f); }
  size_t size() { return e - s;}
  sequence<T> as_sequence() {
    return sequence<T>::tabulate(e-s, [&] (size_t i) {return f(i+s);});
  }
private:
  F *f;
  size_t s, e;
};

// used so second template argument can be inferred
template <class E, class F>
func_sequence<E,F> make_sequence (size_t n, F& f) {
  return func_sequence<E,F>(n,f);
}

template <typename E, typename F>
struct cpy_func_sequence {
  using T = E;
  cpy_func_sequence(size_t n, F _f) : f(_f), s(0), e(n) {};
  cpy_func_sequence(size_t s, size_t e, F _f) : f(_f), s(s), e(e) {};
  T operator[] (const size_t i) {return (f)(i+s);}
  cpy_func_sequence<T,F> slice(size_t ss, size_t ee) {
    return func_sequence<T,F>(s+ss,s+ee,f); }
  size_t size() { return e - s;}
  sequence<T> as_sequence() {
    return sequence<T>::tabulate(e-s, [&] (size_t i) {return f(i+s);});
  }
private:
  F f;
  size_t s, e;
};

// used so second template argument can be inferred
template <class E, class F>
cpy_func_sequence<E,F> make_sequence_cpy (size_t n, F f) {
  return cpy_func_sequence<E,F>(n,f);
}
