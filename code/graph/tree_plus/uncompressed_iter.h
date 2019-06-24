#pragma once

#include "uncompressed_nodes.h"

namespace uncompressed_iter {

  using uchar = unsigned char;

  struct read_iter {
    uintV const* node;
    uintV deg;
    uintV ngh;
    uintV proc;
    bool is_valid;

    read_iter(uintV const* node, uintV src) : node(node), proc(0) {
      is_valid = (node != nullptr);
      if (is_valid) {
        deg = node[proc++];
        assert(deg > 0);
      } else {
        deg = 0;
      }
    }

    bool valid() {
      return is_valid;
    }

    inline uintV cur() {
      return ngh;
    }

    inline uintV next() {
      ngh = node[proc++];
      return ngh;
    }

    inline bool has_next() {
      return (proc < (deg+1));
    }
  };

  struct write_iter {
    uintV* node_ptr;

    uintV src;
    uintV deg_ub;
    size_t proc;
    using allocator_32 = typename uncompressed_lists::node_32::node::allocator;
    using allocator_64 = typename uncompressed_lists::node_64::node::allocator;
    using allocator_128 = typename uncompressed_lists::node_128::node::allocator;
    using allocator_256 = typename uncompressed_lists::node_256::node::allocator;
    using allocator_512 = typename uncompressed_lists::node_512::node::allocator;
    using allocator_1024 = typename uncompressed_lists::node_1024::node::allocator;

    // hacks
    static inline size_t get_block_eq_class(size_t deg) {
      if (deg <= uncompressed_lists::node_32::node::integers_per_block) {
        return 32;
      } else if (deg <= uncompressed_lists::node_64::node::integers_per_block) {
        return 64;
      } else if (deg <= uncompressed_lists::node_128::node::integers_per_block) {
        return 128;
      } else if (deg <= uncompressed_lists::node_256::node::integers_per_block) {
        return 256;
      } else if (deg <= uncompressed_lists::node_512::node::integers_per_block) {
        return 512;
      } else if (deg <= uncompressed_lists::node_1024::node::integers_per_block) {
        return 1024;
      } else {
        return UINT_V_MAX;
      }
    }

    write_iter(uintV src, size_t deg_ub) : src(src), deg_ub(deg_ub) {
      proc = 0;
      node_ptr = uncompressed_lists::alloc_node(deg_ub);
      node_ptr[proc++] = deg_ub; // can recover which allocator was used based on deg.
    }

    void compress_next(const uintV& ngh) {
      node_ptr[proc++] = ngh;
    }

    void finish() {
      size_t actual_deg = proc-1;
      if (get_block_eq_class(actual_deg) != get_block_eq_class(deg_ub)) {
        uintV* new_ptr = uncompressed_lists::alloc_node(actual_deg);
        for (size_t i=0; i<proc; i++) {
          new_ptr[i] = node_ptr[i];
        }
        uncompressed_lists::deallocate(node_ptr); // deg_ub still stored in node_ptr[0]
        node_ptr = new_ptr;
      }
      node_ptr[0] = actual_deg;
    }
  };

}; // namespace uncompressed_iter
