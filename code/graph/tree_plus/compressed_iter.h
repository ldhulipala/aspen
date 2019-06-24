#pragma once

#include "../../common/compression.h"
#include "compressed_nodes_2.h"
//#include "compressed_nodes.h"

namespace compressed_lists {

  size_t node_size(uchar* node) {
    if (node) {
      return *((uint16_t*)node);
    }
    return static_cast<size_t>(0);
  }

}

namespace compressed_iter {

  using namespace compression;
  using uchar = unsigned char;

  struct read_iter {
    uchar const* node;
    uchar const* start;
    uintV src;
    uintV deg;
    uintV ngh;
    uintV proc;
    bool is_valid;

    // uint16_t deg
    // uint16_t size (bytes)
    // uint32_t refct
    // (new): uint32_t last neighbor
    read_iter(uchar const* node, uintV src) : node(node), src(src), deg(0), ngh(0), proc(0) {
      is_valid = (node != nullptr);
      if (is_valid) {
        deg = *((uint16_t*)node);
        assert(deg > 0);
        start = node + 2*sizeof(uint16_t) + sizeof(uint32_t) + sizeof(uint32_t);
      }
    }

    bool valid() {
      return is_valid;
    }

    inline uintV last_key() {
      uint32_t* last_ngh_ptr = (uint32_t*)(node + 2*sizeof(uint16_t) + sizeof(uint32_t));
      return *last_ngh_ptr;
    }

    inline uintV next() {
      if (proc == 0) {
        ngh = read_first_neighbor(start, src);
      } else {
        ngh += read_neighbor(start);
      }
      proc++;
      return ngh;
    }

    inline bool has_next() {
      return proc < deg;
    }
  };

  struct write_iter {
    uchar* node_ptr;
    size_t offset = 0;
    size_t proc = 0;
    size_t node_size;

    uintV src; uintV last_ngh;
    uintV deg_ub;

    // format:
    // uint16_t: deg
    // uint16_t: total size in bytes
    write_iter(uintV src, size_t deg_ub) : src(src), deg_ub(deg_ub) {
      proc = 0;
      std::tie(node_ptr, node_size) = compressed_lists::alloc_node(deg_ub*5 + 2 + 2 + 4 + 4); // 2 uint16_t's and 2 uint32_t
      offset += sizeof(uint16_t) + sizeof(uint16_t) + 2*sizeof(uint32_t);
    }

    void compress_next(const uintV& ngh) {
      if (proc == 0) {
        offset = compress_first_neighbor(node_ptr, offset, src, ngh);
      } else {
        uintV difference = ngh - last_ngh;
        offset = compress_neighbor(node_ptr, offset, difference);
      }
      proc++;
      last_ngh = ngh;
    }

    uchar* finish() {
      // write the last neighbor
      if (offset > node_size) {
        cout << "offset = " << offset << " node-size = " << node_size << " src = " << src << " deg = " << deg_ub << " proc = " << proc << endl;
        cout << endl;
        cout << endl;
        assert(offset < node_size);
      }

      uint16_t* deg = (uint16_t*)node_ptr;
      uint16_t* total_size = (uint16_t*)(node_ptr + sizeof(uint16_t));
      uint32_t* ref_ct = (uint32_t*)(node_ptr + 2*sizeof(uint16_t));
      uint32_t* last_ngh_ptr = (uint32_t*)(node_ptr + 2*sizeof(uint16_t) + sizeof(uint32_t));

      *deg = proc;
      *total_size = node_size;
      *ref_ct = 1;
      *last_ngh_ptr = last_ngh;
      if (proc == 0) {
        compressed_lists::deallocate(node_ptr);
        return nullptr;
      }
#ifndef NDEBUG // check that the allocated sizes are fine for uint16_t's
      if (proc > (1 << 16)) {
        cout << "Need to resize" << endl;
        cout << "proc = " << proc << endl;
        exit(0);
      }
#endif
      if (2*offset <= node_size) {
        uchar* old_arr = node_ptr;
        std::tie(node_ptr, node_size) = compressed_lists::alloc_node(offset);
        std::memcpy(node_ptr, old_arr, offset);
        total_size = (uint16_t*)(node_ptr + sizeof(uint16_t));
        compressed_lists::deallocate(old_arr);
      }
      *total_size = offset;
      return node_ptr;
    }
  };

}; // namespace compressed_iter
