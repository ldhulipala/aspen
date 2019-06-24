#pragma once
#include "../pbbslib/get_time.h"
#include "../pbbslib/sequence_ops.h"
#include "../pbbslib/sample_sort.h"

template <class Entry>
struct build {
  using K = typename Entry::key_t;
  using V = typename Entry::val_t;
  using ET = typename Entry::entry_t;

  static sequence<ET>
  sort_remove_duplicates(sequence<ET> A,
			 bool seq_inplace = false, bool inplace = false) {
    auto less = [&] (ET a, ET b) {
      return Entry::comp(Entry::get_key(a), Entry::get_key(b));};
    if (A.size() == 0) return sequence<ET>(0);
    if (seq_inplace) {
      pbbs::quicksort(A.as_array(), A.size(), less);

      // remove duplicates
      size_t  j = 1;
      for (size_t i=1; i<A.size(); i++)
	if (less(A[j-1], A[i])) A[j++] = A[i];
      return A.slice(0,j);
    } else {
	  if (!inplace) {
		  auto B = pbbs::sample_sort(A, less);
		  sequence<bool> Fl(B.size(), [&] (size_t i) {
		  return (i==0) || less(B[i-1], B[i]); });
		  auto C = pbbs::pack(B, Fl);
		  return C;
	  } else {
		  pbbs::sample_sort(A.as_array(), A.size(), less);
		  //sequence<bool> Fl(A.size(), [&] (size_t i) { return (i==0) || less(A[i-1], A[i]); });
		  //auto C = pbbs::pack(A, Fl);
		  auto g = [&] (size_t i) {return (i==0) || less(A[i-1], A[i]); };
		  auto C = pbbs::pack(A, make_sequence<bool>(A.size(),g));
		  return C;
	  }
    }
  }

  sequence<ET>
  static sort_remove_duplicates(ET* A, size_t n) {
    auto lessE = [&] (ET a, ET b) {
      return Entry::comp(a.first, b.first);};

	pbbs::sample_sort(A, n, lessE);

	//sequence<bool> Fl(n, [&] (size_t i) {return (i==0) || lessE(A[i-1], A[i]); });

	auto f = [&] (size_t i) {return A[i];};
	auto g = [&] (size_t i) {return (i==0) || lessE(A[i-1], A[i]); };
    //auto B = pbbs::pack(make_sequence<ET>(n,f), Fl);
	auto B = pbbs::pack(make_sequence<ET>(n,f), make_sequence<bool>(n,g));

	//sequence<ET> B(I.size(),
	  //[&] (size_t i) { return A[I[i]]; });

	return B;

  }

  template<class Vin, class Reduce>
  std::pair<ET*, size_t>
  static sort_reduce_duplicates(std::pair<K,Vin>* A, size_t n, Reduce& reduce, bool sorted=false) {
    using E = std::pair<K,Vin>;
    auto lessE = [&] (E a, E b) {
      return Entry::comp(a.first, b.first);};

    if (!sorted) {
      pbbs::sample_sort(A, n, lessE);
    }

    // determines the index of start of each block of equal keys
    // and copies values into vals
    bool* is_start = new bool[n];
    Vin* vals = pbbs::new_array_no_init<Vin>(n);
    is_start[0] = 1;
    vals[0] = A[0].second;
    parallel_for(size_t i = 1; i < n; i++) {
      is_start[i] = Entry::comp(A[i-1].first, A[i].first);
      new (static_cast<void*>(vals+i)) Vin(A[i].second);
    }

    sequence<node_size_t> I =
      pbbs::pack_index<node_size_t>(sequence<bool>(is_start,n));
    delete[] is_start;

    // combines over each block of equal keys using function reduce
    ET* B = pbbs::new_array<ET>(I.size());
    parallel_for(size_t i = 0; i < I.size(); i++) {
      size_t start = I[i];
      size_t end = (i==I.size()-1) ? n : I[i+1];
      B[i] = ET(A[start].first, reduce(A[start].first, vals+start,vals+end));
    }

    pbbs::delete_array(vals, n);
    return std::pair<ET*,size_t>(B,I.size());
  }


  template<class Vi, class Reduce>
  static sequence<ET>
  sort_reduce_duplicates(sequence<std::pair<K,Vi>> A,
			 const Reduce& reduce, bool sorted=false) {
    using E = std::pair<K,Vi>;
    size_t n = A.size();
    if (n == 0) return sequence<ET>(0);
	//timer t, t1, t2;
    auto lessE = [&] (E a, E b) {
      return Entry::comp(a.first, b.first);};
    if (!sorted) {
      pbbs::sample_sort(A.as_array(), n, lessE);
    }

    sequence<E> B(A.as_array(), A.as_array()+n);
    sequence<bool> Fl(n);

    // determines the index of start of each block of equal keys
    // and copies values into vals
    sequence<Vi> Vals(n, [&] (size_t i) {
	Fl[i] = (i==0) || lessE(B[i-1], B[i]);
	return B[i].second;
      });

	//t1.start();
    sequence<node_size_t> I = pbbs::pack_index<node_size_t>(Fl);
	//std::cout << "time pack: " << t1.stop() << std::endl;

    // combines over each block of equal keys using function reduce
    auto set = [&] (size_t i) {
      size_t start = I[i];
      size_t end = (i==I.size()-1) ? n : I[i+1];
      return ET(B[start].first, reduce(B[start].first, Vals.slice(start,end)));
    };
	//t2.start();
    auto a =  sequence<ET>(I.size(), set);
	//std::cout << "time reduce: " << t2.stop() << std::endl;

    // tabulate set over all entries of i
    return a;
  }

  template<class Bin_Op>
  static sequence<ET>
  sort_combine_duplicates(sequence<ET> A,
			  Bin_Op& f,
			  bool seq_inplace=false) {
    auto less = [&] (ET a, ET b) {return Entry::comp(a.first, b.first);};
    if (A.size() == 0) return sequence<ET>(0);
    if (seq_inplace) {
      pbbs::quicksort(A.as_array(), A.size(), less);
      size_t j = 0;
      for (size_t i=1; i < A.size(); i++) {
	if (less(A[j], A[i])) A[++j] = A[i];
	else A[j].second = f(A[j].second,A[i].second);
      }
      return A.slice(0,j+1);
    } else {
      auto reduce_op = [&] (K k, sequence<V> S) { return pbbs::reduce(S, f);};
      return sort_reduce_duplicates(A, reduce_op);
    }
  }
};
