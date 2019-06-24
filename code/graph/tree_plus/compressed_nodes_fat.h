#pragma once

#include <cstring>

#define def_node(x) \
  namespace node_##x { \
    struct node { \
      static constexpr const size_t node_array_size = x; \
      uchar neighbors[node_array_size]; \
      using allocator = list_allocator<node>; \
      node() { } \
      node(const node& other) { \
        auto their_array = other.neighbors; \
        std::memcpy(neighbors, their_array, node_array_size); \
      } \
    }; \
  } \

#define init_node(x, div_by, amt) \
    using node_##x = node_##x::node; \
    using node_##x##_gc = gc<node_##x>; \
    node_##x##_gc::init(n/div_by, (1 << amt)); \

#define using_alloc(x) \
  using allocator_##x = typename node_##x::node::allocator;

#define if_branch_size(x) \
  else if (num_bytes <= node_##x::node::node_array_size) { \
    return x; \
  } \

#define if_branch_free(x) \
  else if (num_bytes <= node_##x::node::node_array_size) { \
        allocator_##x::free(static_cast<node_##x::node*>(node)); \
    } \

#define if_branch_alloc(x) \
    else if (num_bytes <= node_##x::node::node_array_size) { \
      return make_tuple((uchar*)allocator_##x::alloc(), x); \
    } \

#define print_alloc_stats(x) \
  cout << "lists##x: "; allocator_##x::print_stats();

#define add_total_bytes(x) \
    allocator_##x::num_used_bytes() +


namespace compressed_lists {

  def_node(8)
  def_node(16)
  def_node(32)
  def_node(64)
  def_node(128)
  def_node(256)
  def_node(512)
  def_node(1024)
  def_node(2048)
  def_node(4096)
  def_node(8192)
  def_node(16384)


  void init(size_t n) {
    using node_8 = node_8::node;
    using node_8_gc = gc<node_8>;
    node_8_gc::init(n/256, (1 << 16));

    using node_16 = node_16::node;
    using node_16_gc = gc<node_16>;
    node_16_gc::init(n/256, (1 << 16));

    using node_32 = node_32::node;
    using node_32_gc = gc<node_32>;
    node_32_gc::init(n/256, (1 << 16));

    using node_64 = node_64::node;
    using node_64_gc = gc<node_64>;
    node_64_gc::init(n/256, (1 << 16));

    using node_128 = node_128::node;
    using node_128_gc = gc<node_128>;
    node_128_gc::init(n/256, (1 << 15));

    using node_256 = node_256::node;
    using node_256_gc = gc<node_256>;
    node_256_gc::init(n/256, (1 << 14));

    using node_512 = node_512::node;
    using node_512_gc = gc<node_512>;
    node_512_gc::init(n/512, (1 << 13));

    using node_1024 = node_1024::node;
    using node_1024_gc = gc<node_1024>;
    node_1024_gc::init(n/1024, (1 << 12));

    using node_2048 = node_2048::node;
    using node_2048_gc = gc<node_2048>;
    node_2048_gc::init(n/2048, (1 << 11));

    using node_4096 = node_4096::node;
    using node_4096_gc = gc<node_4096>;
    node_4096_gc::init(n/4096, (1 << 10));

    using node_8192 = node_8192::node;
    using node_8192_gc = gc<node_8192>;
    node_8192_gc::init(n/8192, (1 << 9));

    using node_16384 = node_16384::node;
    using node_16384_gc = gc<node_16384>;
    node_16384_gc::init(n/16384, (1 << 8));
  }

  using allocator_8 = typename node_8::node::allocator;
  using allocator_16 = typename node_16::node::allocator;
  using allocator_32 = typename node_32::node::allocator;
  using allocator_64 = typename node_64::node::allocator;
  using allocator_128 = typename node_128::node::allocator;
  using allocator_256 = typename node_256::node::allocator;
  using allocator_512 = typename node_512::node::allocator;
  using allocator_1024 = typename node_1024::node::allocator;
  using allocator_2048 = typename node_2048::node::allocator;
  using allocator_4096 = typename node_4096::node::allocator;
  using allocator_8192 = typename node_8192::node::allocator;
  using allocator_16384 = typename node_16384::node::allocator;

  void print_stats() {
    cout <<"compressed lists 2 printing stats" << endl;

    cout << "lists8: "; allocator_8::print_stats();
    cout << "lists16: "; allocator_16::print_stats();
    cout << "lists32: "; allocator_32::print_stats();
    cout << "lists64: "; allocator_64::print_stats();
    cout << "lists128: "; allocator_128::print_stats();
    cout << "lists256: "; allocator_256::print_stats();
    cout << "lists512: "; allocator_512::print_stats();
    cout << "lists1024: "; allocator_1024::print_stats();
    cout << "lists2048: "; allocator_2048::print_stats();
    cout << "lists4096: "; allocator_4096::print_stats();
    cout << "lists8192: "; allocator_8192::print_stats();
    cout << "lists16384: "; allocator_16384::print_stats();
    size_t total_bytes = allocator_8::num_used_bytes() +
      allocator_16::num_used_bytes() +
      allocator_32::num_used_bytes() +
      allocator_64::num_used_bytes() +
      allocator_128::num_used_bytes() +
      allocator_256::num_used_bytes() +
      allocator_512::num_used_bytes() +
      allocator_1024::num_used_bytes() +
      allocator_2048::num_used_bytes() +
      allocator_4096::num_used_bytes() +
      allocator_8192::num_used_bytes() +
      allocator_16384::num_used_bytes();
    cout << "Total bytes for node allocators: " << total_bytes << endl;
  }

  size_t get_used_bytes() {
    size_t total_bytes = allocator_8::num_used_bytes() +
      allocator_16::num_used_bytes() +
      allocator_32::num_used_bytes() +
      allocator_64::num_used_bytes() +
      allocator_128::num_used_bytes() +
      allocator_256::num_used_bytes() +
      allocator_512::num_used_bytes() +
      allocator_1024::num_used_bytes() +
      allocator_2048::num_used_bytes() +
      allocator_4096::num_used_bytes() +
      allocator_8192::num_used_bytes() +
      allocator_16384::num_used_bytes();
    return total_bytes;
  }

  inline uintV underlying_array_size(uchar* node_int) {
    if (node_int) {
      size_t num_bytes = *((uint32_t*)(node_int + sizeof(uint32_t)));
      if (num_bytes <= node_8::node::node_array_size) {
        return 8;
      } else if (num_bytes <= node_16::node::node_array_size) {
        return 16;
      } else if (num_bytes <= node_32::node::node_array_size) {
        return 32;
      } else if (num_bytes <= node_64::node::node_array_size) {
        return 64;
      } else if (num_bytes <= node_128::node::node_array_size) {
        return 128;
      } else if (num_bytes <= node_256::node::node_array_size) {
        return 256;
      } else if (num_bytes <= node_512::node::node_array_size) {
        return 512;
      } else if (num_bytes <= node_1024::node::node_array_size) {
        return 1024;
      } else if (num_bytes <= node_2048::node::node_array_size) {
        return 2048;
      } else if (num_bytes <= node_4096::node::node_array_size) {
        return 4096;
      } else if (num_bytes <= node_8192::node::node_array_size) {
        return 8192;
      } else if (num_bytes <= node_16384::node::node_array_size) {
        return 16384;
      }
      else {
        return num_bytes;
      }
    }
    return 0;
  }

  inline bool deallocate(uchar* node_int) {
    if (node_int) {
      uint32_t* ref_ct = (uint32_t*)(node_int + sizeof(uint32_t) + sizeof(uint32_t));
      if (pbbs::fetch_and_add(ref_ct, -1) > 1) {
        return false;
      }
      size_t num_bytes = *((uint32_t*)(node_int + sizeof(uint32_t)));
      void* node = static_cast<void*>(node_int);
      if (num_bytes <= node_8::node::node_array_size) {
        allocator_8::free(static_cast<node_8::node*>(node));
      } else if (num_bytes <= node_16::node::node_array_size) {
        allocator_16::free(static_cast<node_16::node*>(node));
      } else if (num_bytes <= node_32::node::node_array_size) {
        allocator_32::free(static_cast<node_32::node*>(node));
      } else if (num_bytes <= node_64::node::node_array_size) {
        allocator_64::free(static_cast<node_64::node*>(node));
      } else if (num_bytes <= node_128::node::node_array_size) {
        allocator_128::free(static_cast<node_128::node*>(node));
      } else if (num_bytes <= node_256::node::node_array_size) {
        allocator_256::free(static_cast<node_256::node*>(node));
      } else if (num_bytes <= node_512::node::node_array_size) {
        allocator_512::free(static_cast<node_512::node*>(node));
      } else if (num_bytes <= node_1024::node::node_array_size) {
        allocator_1024::free(static_cast<node_1024::node*>(node));
      } else if (num_bytes <= node_2048::node::node_array_size) {
        allocator_2048::free(static_cast<node_2048::node*>(node));
      } else if (num_bytes <= node_4096::node::node_array_size) {
        allocator_4096::free(static_cast<node_4096::node*>(node));
      } else if (num_bytes <= node_8192::node::node_array_size) {
        allocator_8192::free(static_cast<node_8192::node*>(node));
      } else if (num_bytes <= node_16384::node::node_array_size) {
        allocator_16384::free(static_cast<node_16384::node*>(node));
      }
      else {
//        cout << "Freeing node alloc'd with malloc." << endl;
//        cout << "Freeing " << ((size_t) node) << endl;
        pbbs::free_array(node);
      }
      return true;
    }
    return false;
  }

  inline tuple<uchar*, size_t> alloc_node(size_t num_bytes) {
    if (num_bytes <= node_8::node::node_array_size) {
      return make_tuple((uchar*)allocator_8::alloc(), 8);
    } else if (num_bytes <= node_16::node::node_array_size) {
      return make_tuple((uchar*)allocator_16::alloc(), 16);
    } else if (num_bytes <= node_32::node::node_array_size) {
      return make_tuple((uchar*)allocator_32::alloc(), 32);
    } else if (num_bytes <= node_64::node::node_array_size) {
      return make_tuple((uchar*)allocator_64::alloc(), 64);
    } else if (num_bytes <= node_128::node::node_array_size) {
      return make_tuple((uchar*)allocator_128::alloc(), 128);
    } else if (num_bytes <= node_256::node::node_array_size) {
      return make_tuple((uchar*)allocator_256::alloc(), 256);
    } else if (num_bytes <= node_512::node::node_array_size) {
      return make_tuple((uchar*)allocator_512::alloc(), 512);
    } else if (num_bytes <= node_1024::node::node_array_size) {
      return make_tuple((uchar*)allocator_1024::alloc(), 1024);
    } else if (num_bytes <= node_2048::node::node_array_size) {
      return make_tuple((uchar*)allocator_2048::alloc(), 2048);
    } else if (num_bytes <= node_4096::node::node_array_size) {
      return make_tuple((uchar*)allocator_4096::alloc(), 4096);
    } else if (num_bytes <= node_8192::node::node_array_size) {
      return make_tuple((uchar*)allocator_8192::alloc(), 8192);
    } else if (num_bytes <= node_16384::node::node_array_size) {
      return make_tuple((uchar*)allocator_16384::alloc(), 16384);
    }
    else {
//      cout << "malloc: num_bytes = " << num_bytes << endl;
//      auto node = make_tuple((uchar*)(pbbs::new_array_no_init<uchar>(num_bytes)), num_bytes);
      auto node = make_tuple((uchar*)(pbbs::new_array_no_init<uchar>(num_bytes)), num_bytes);
//      cout << "alloc: = " << ((size_t)std::get<0>(node)) << endl;
      return node;
    }
  }

}
