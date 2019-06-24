#pragma once

#include "../pbbslib/utilities.h"

#include <limits.h>

template <class intT>
struct rMat {
  double a, ab, abc;
  intT n;
  intT h;
  rMat(intT _n, intT _seed,
       double _a, double _b, double _c) {
    n = _n; a = _a; ab = _a + _b; abc = _a+_b+_c;
    h = pbbs::hash32((intT)_seed);
    if(abc > 1) { cout << "in rMat: a + b + c add to more than 1\n"; abort();}
    if((1UL << pbbs::log2_up(n)) != n) { cout << "in rMat: n not a power of 2"; abort(); }
  }


  double hashDouble(intT i) {
    return ((double) (pbbs::hash32((intT)i))/((double) std::numeric_limits<intV>::max()));}

  std::pair<intT, intT> rMatRec(intT nn, intT randStart, intT randStride) {
    if (nn==1) return std::make_pair<intT, intT>(0,0);
    else {
      std::pair<intT, intT> x = rMatRec(nn/2, randStart + randStride, randStride);
      double r = hashDouble(randStart);
      if (r < a) return x;
      else if (r < ab) return std::make_pair(x.first,x.second+nn/2);
      else if (r < abc) return std::make_pair(x.first+nn/2, x.second);
      else return std::make_pair(x.first+nn/2, x.second+nn/2);
    }
  }

  std::pair<intT, intT> operator() (intT i) {
    intT randStart = pbbs::hash32((intT)(2*i)*h);
    intT randStride = pbbs::hash32((intT)(2*i+1)*h);
    return rMatRec(n, randStart, randStride);
  }
};

