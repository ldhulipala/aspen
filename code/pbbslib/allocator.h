// This code is part of the Problem Based Benchmark Suite (PBBS)
// Copyright (c) 2016 Guy Blelloch, Daniel Ferizovic, and the PBBS team
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights (to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

// A concurrent allocator for any fixed type T
// Keeps a local pool per processor
// Grabs list_size elements from a global pool if empty, and
// Returns list_size elements to the global pool when local pool=2*list_size
// Keeps track of number of allocated elements.
// Probably more efficient than a general purpose allocator

#pragma once

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include "utilities.h"
#include "block_allocator.h"

using namespace pbbs;

struct small_allocator {

  static const int log_min_size = 4;
  static const int total_list_size = (1 << 22);
  static const int header_size = 16;
  int log_max_size = 17;
  int num_buckets = log_max_size - log_min_size + 1;
  struct block_allocator *allocators;

  ~small_allocator() {
    //for (int i=0; i < num_buckets; i++)
    //  ~block_allocator(allocators[i]));
    //free(allocators);
  }

<<<<<<< HEAD
  small_allocator() {}
  small_allocator(size_t log_max_size) : log_max_size(log_max_size) ,
					 num_buckets(log_max_size - log_min_size + 1) 
  {
    allocators = (struct block_allocator*)
      malloc(num_buckets * sizeof(struct block_allocator));
    
    for (int i = 0; i < num_buckets; i++) {
      size_t bucket_size = ((size_t) 1) << (i+log_min_size);
      size_t per_proc_list_size = total_list_size / bucket_size;
      size_t initial_additional_blocks = 0;
      new (static_cast<void*>(std::addressof(allocators[i]))) 
	block_allocator(bucket_size, initial_additional_blocks,
			per_proc_list_size);
    }
  }

  void* alloc_log(int log_size) {
=======
  void* alloc(size_t n) {
    size_t np = n + header_size;  // add header

    size_t log_size = pbbs::log2_up((size_t) np);
>>>>>>> 6fafa08db81d476efee30d086c47d9ef8bf8a4c8
    if (log_size > log_max_size) abort();
    int bucket = std::max(0, log_size - log_min_size);
    void* ptr = allocators[bucket].alloc();
    *((int*) ptr) = log_size;
    return ((char*) ptr) + header_size;
  }

  void* alloc(size_t n) {
    int log_size = pbbs::log2_up(n + header_size);
    return alloc_log(log_size);
  }

  void free(void* ptr) {
    char* head = ((char*) ptr) - header_size;
    int log_size = *((int*) head);
    int bucket = std::max(0, log_size - log_min_size);
    allocators[bucket].free(head);
  }

  void print_stats() {
    size_t total_a = 0;
    size_t total_u = 0;
    for (int i = 0; i < num_buckets; i++) {
      size_t bucket_size = ((size_t) 1) << (i+log_min_size);
      size_t allocated = allocators[i].num_allocated_blocks();
      size_t used = allocators[i].num_used_blocks();
      total_a += allocated * bucket_size;
      total_u += used * bucket_size;
      cout << "size = " << bucket_size << ", allocated = " << allocated
	   << ", used = " << used << endl;
    }
    cout << "Total bytes allocated = " << total_a << endl;
    cout << "Total bytes used = " << total_u << endl;
  }
    
};
