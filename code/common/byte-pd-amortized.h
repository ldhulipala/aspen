#pragma once

#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <cmath>
#include <stdio.h>
#include <string.h>

namespace bytepd_amortized {

#define PARALLEL_DEGREE 1000

  inline size_t get_virtual_degree(uintV d, uchar* ngh_arr) {
    if (d > 0) {
      return *((uintV*)ngh_arr);
    }
    return 0;
  }

  inline uintV eatFirstEdge(uchar* &start, uintV source) {
    uchar fb = *start++;
    uintV edgeRead = (fb & 0x3f);
    if (LAST_BIT_SET(fb)) {
      int shiftAmount = 6;
      while (1) {
        uchar b = *start;
        edgeRead |= ((b & 0x7f) << shiftAmount);
        start++;
        if (LAST_BIT_SET(b))
          shiftAmount += EDGE_SIZE_PER_BYTE;
        else
          break;
      }
    }
    return (fb & 0x40) ? source - edgeRead : source + edgeRead;
  }

  inline uintV eatEdge(uchar* &start) {
    uintV edgeRead = 0;
    int shiftAmount = 0;

    while (1) {
      uchar b = *start;
      edgeRead += ((b & 0x7f) << shiftAmount);
      start++;
      if (LAST_BIT_SET(b))
        shiftAmount += EDGE_SIZE_PER_BYTE;
      else
        break;
    }
    return edgeRead;
  }

  struct simple_iter {
    uchar* base;
    uchar* finger;
    uintV src;
    uintE degree;

    uintV num_blocks;
    uintV cur_chunk;
    uintV last_edge;

    uintV proc;

    simple_iter(uchar* _base, uintE _degree, uintV _src) : base(_base), src(_src), degree(_degree), 
                                                    cur_chunk(0) {
      if (degree == 0) return;
      uintV virtual_degree = *((uintV*)base);
      num_blocks = 1+(virtual_degree-1)/PARALLEL_DEGREE;
 //     uintV* block_offsets = (uintV*)(base + sizeof(uintV));

      finger = base + (num_blocks-1)*sizeof(uintV) + sizeof(uintV);

//      uintV start_offset = *((uintV*)finger);
//      uintV end_offset = (0 == (num_blocks-1)) ? degree : (*((uintV*)(base+block_offsets[0])));
      finger += sizeof(uintV);
      last_edge = eatFirstEdge(finger, src);
      proc = 1;
    }

    inline uintV cur() {
      return last_edge;
    }

    inline uintV next() {
      if (proc == PARALLEL_DEGREE) {
        finger += sizeof(uintV); // skip block start
        last_edge = eatFirstEdge(finger, src);
        proc = 1;
        cur_chunk++;
      } else {
        last_edge += eatEdge(finger);
        proc++;
      }
      return last_edge;
    }

    inline bool has_next() {
      return (cur_chunk*PARALLEL_DEGREE + proc) < degree;
    }
  };

};  // namespace bytepd_amortized
