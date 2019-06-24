#pragma once

namespace uncompressed_lists {
  namespace node_32 {
    struct node {
      static constexpr const size_t node_array_size = 32;
      static constexpr const size_t integers_per_block = node_array_size - 1;
      uintV neighbors[node_array_size];
      using allocator = list_allocator<node>;

      node() { }

      node(const node& other) {
        uintV const* their_array = other.neighbors;
        for (size_t i=0; i<node_array_size; i++) {
          neighbors[i] = their_array[i];
        }
      }
    };
  }

  namespace node_64 {
    struct node {
      static constexpr const size_t node_array_size = 64;
      static constexpr const size_t integers_per_block = node_array_size - 1;
      uintV neighbors[node_array_size];
      using allocator = list_allocator<node>;

      node() { }

      node(const node& other) {
        uintV const* their_array = other.neighbors;
        for (size_t i=0; i<node_array_size; i++) {
          neighbors[i] = their_array[i];
        }
      }
    };
  }


  namespace node_128 {
    struct node {
      static constexpr const size_t node_array_size = 128;
      static constexpr const size_t integers_per_block = node_array_size - 1;
      uintV neighbors[node_array_size];
      using allocator = list_allocator<node>;

      node() { }

      node(const node& other) {
        uintV const* their_array = other.neighbors;
        for (size_t i=0; i<node_array_size; i++) {
          neighbors[i] = their_array[i];
        }
      }
    };
  }

  namespace node_256 {
    struct node {
      static constexpr const size_t node_array_size = 256;
      static constexpr const size_t integers_per_block = node_array_size - 1;
      uintV neighbors[node_array_size];
      using allocator = list_allocator<node>;

      node() { }

      node(const node& other) {
        uintV const* their_array = other.neighbors;
        for (size_t i=0; i<node_array_size; i++) {
          neighbors[i] = their_array[i];
        }
      }
    };
  }

  namespace node_512 {
    struct node {
      static constexpr const size_t node_array_size = 512;
      static constexpr const size_t integers_per_block = node_array_size - 1;
      uintV neighbors[node_array_size];
      using allocator = list_allocator<node>;

      node() { }

      node(const node& other) {
        uintV const* their_array = other.neighbors;
        for (size_t i=0; i<node_array_size; i++) {
          neighbors[i] = their_array[i];
        }
      }
    };
  }

  namespace node_1024 {
    struct node {
      static constexpr const size_t node_array_size = 1024;
      static constexpr const size_t integers_per_block = node_array_size - 1;
      uintV neighbors[node_array_size];
      using allocator = list_allocator<node>;

      node() { }

      node(const node& other) {
        uintV const* their_array = other.neighbors;
        for (size_t i=0; i<node_array_size; i++) {
          neighbors[i] = their_array[i];
        }
      }
    };
  }

  void init(size_t n) {
    using node_32 = node_32::node;
    using node_32_gc = gc<node_32>;
    node_32_gc::init(n/32);

    using node_64 = node_64::node;
    using node_64_gc = gc<node_64>;
    node_64_gc::init(n/64);

    using node_128 = node_128::node;
    using node_128_gc = gc<node_128>;
    node_128_gc::init(n/128);

    using node_256 = node_256::node;
    using node_256_gc = gc<node_256>;
    node_256_gc::init(n/256);

    using node_512 = node_512::node;
    using node_512_gc = gc<node_512>;
    node_512_gc::init(n/512);

    using node_1024 = node_1024::node;
    using node_1024_gc = gc<node_1024>;
    node_1024_gc::init(n/1024);
  }

  using allocator_32 = typename node_32::node::allocator;
  using allocator_64 = typename node_64::node::allocator;
  using allocator_128 = typename node_128::node::allocator;
  using allocator_256 = typename node_256::node::allocator;
  using allocator_512 = typename node_512::node::allocator;
  using allocator_1024 = typename node_1024::node::allocator;

  void print_stats() {
    cout << "lists32: "; node_32::node::allocator::print_stats();
    cout << "lists64: "; node_64::node::allocator::print_stats();
    cout << "lists128: "; node_128::node::allocator::print_stats();
    cout << "lists256: "; node_256::node::allocator::print_stats();
    cout << "lists512: "; node_512::node::allocator::print_stats();
    cout << "lists1024: "; node_1024::node::allocator::print_stats();
  }

  inline void deallocate(uintV* node_int) {
    if (node_int) {
      uintV deg = node_int[0];
      void* node = static_cast<void*>(node_int);
      if (deg <= node_32::node::integers_per_block) {
        allocator_32::free(static_cast<node_32::node*>(node));
      } else if (deg <= node_64::node::integers_per_block) {
        allocator_64::free(static_cast<node_64::node*>(node));
      } else if (deg <= node_128::node::integers_per_block) {
        allocator_128::free(static_cast<node_128::node*>(node));
      } else if (deg <= node_256::node::integers_per_block) {
        allocator_256::free(static_cast<node_256::node*>(node));
      } else if (deg <= node_512::node::integers_per_block) {
        allocator_512::free(static_cast<node_512::node*>(node));
      } else if (deg <= node_1024::node::integers_per_block) {
        allocator_1024::free(static_cast<node_1024::node*>(node));
      } else {
        cout << "Freeing node alloc'd with malloc." << endl;
        pbbs::free_array(node);
      }
    }
  }

  size_t get_used_bytes() {
    return node_32::node::allocator::num_used_bytes() +
    node_64::node::allocator::num_used_bytes() +
    node_128::node::allocator::num_used_bytes() +
    node_256::node::allocator::num_used_bytes() +
    node_512::node::allocator::num_used_bytes() +
     node_1024::node::allocator::num_used_bytes();
  }

  inline uintV underlying_array_size(uintV* node_int) {
    return 0;
  }

  inline uintV* alloc_node(size_t deg) {
    if (deg <= node_32::node::integers_per_block) {
      return (uintV*)allocator_32::alloc();
    } else if (deg <= node_64::node::integers_per_block) {
      return (uintV*)allocator_64::alloc();
    } else if (deg <= node_128::node::integers_per_block) {
      return (uintV*)allocator_128::alloc();
    } else if (deg <= node_256::node::integers_per_block) {
      return (uintV*)allocator_256::alloc();
    } else if (deg <= node_512::node::integers_per_block) {
      return (uintV*)allocator_512::alloc();
    } else if (deg <= node_1024::node::integers_per_block) {
      return (uintV*)allocator_1024::alloc();
    } else {
      size_t arr_size = deg + 1; // check
      cout << "malloc: deg = " << deg << endl;
      return (uintV*)(pbbs::new_array_no_init<uintV>(arr_size));
    }
  }

}
