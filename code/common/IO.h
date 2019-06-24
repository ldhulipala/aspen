#pragma once

#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <cmath>
#include <sys/mman.h>
#include <stdio.h>
#include <malloc.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

#include "../pbbslib/strings/string_basics.h"
#include "../pbbslib/sequence_ops.h"
#include "../pbbslib/monoid.h"

#include "compression.h"

using namespace std;

std::pair<char*, size_t> mmapStringFromFile(const char *filename) {
  struct stat sb;
  int fd = open(filename, O_RDONLY);
  if (fd == -1) {
    perror("open");
    exit(-1);
  }
  if (fstat(fd, &sb) == -1) {
    perror("fstat");
    exit(-1);
  }
  if (!S_ISREG (sb.st_mode)) {
    perror("not a file\n");
    exit(-1);
  }
  char *p = static_cast<char*>(mmap(0, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
  if (p == MAP_FAILED) {
    perror("mmap");
    exit(-1);
  }
  if (close(fd) == -1) {
    perror("close");
    exit(-1);
  }
  size_t n = sb.st_size;
  return std::make_pair(p, n);
}

pbbs::sequence<char> readStringFromFile(const char* fileName) {
  std::ifstream file(fileName, std::ios::in | std::ios::binary | std::ios::ate);
  if (!file.is_open()) {
    std::cout << "Unable to open file: " << fileName << "\n";
    abort();
  }
  uint64_t end = file.tellg();
  file.seekg(0, std::ios::beg);
  uint64_t n = end - file.tellg();
  auto bytes = pbbs::sequence<char>(n); // n+1?
  file.read(bytes.begin(), n);
  file.close();
  return bytes;
}

auto read_unweighted_graph(const char* fname, bool is_symmetric, bool mmap=false) {
  pbbs::sequence<char*> tokens;
  pbbs::sequence<char> S;
  if (mmap) {
    auto SS = mmapStringFromFile(fname);
    char *bytes = pbbs::new_array_no_init<char>(SS.second);
    // Cannot mutate the graph unless we copy.
    parallel_for(0, SS.second, [&] (size_t i) {
      bytes[i] = SS.first[i];
    });
    if (munmap(SS.first, SS.second) == -1) {
      perror("munmap");
      exit(-1);
    }
    S = pbbs::sequence<char>(bytes, SS.second);
  } else {
    S = readStringFromFile(fname);
  }
  tokens = pbbs::tokenize(S, [] (const char c) { return pbbs::is_space(c); });
  assert(tokens[0] == (string) "AdjacencyGraph");

  size_t len = tokens.size() - 1;
  size_t n = atol(tokens[1]);
  size_t m = atol(tokens[2]);

  cout << "n = " << n << " m = " << m << endl;
  assert(len == n + m + 2);

  uintE* offsets = pbbs::new_array_no_init<uintE>(n);
  uintV* edges = pbbs::new_array_no_init<uintV>(m);

  parallel_for(0, n, [&] (size_t i) { offsets[i] = atol(tokens[i + 3]); });
  parallel_for(0, m, [&] (size_t i) {
    edges[i] = atol(tokens[i+n+3]);
  });

  S.clear();
  tokens.clear();
  return make_tuple(n, m, offsets, edges);
}

auto read_o_direct(const char* fname) {
  int fd;
  if ( (fd = open(fname, O_RDONLY | O_DIRECT) ) != -1) {
    cout << "input opened!" << endl;
  } else {
    cout << "can't open input file!";
  }

  size_t fsize = lseek(fd, 0, SEEK_END);
  lseek(fd, 0, 0);
  auto s = (char*)memalign(4096 * 2, fsize + 4096);

  cout << "fsize = " << fsize << endl;

  size_t sz = 0;

  size_t pgsize = getpagesize();
  cout << "pgsize = " << pgsize << endl;

  size_t read_size = 1024*1024*1024;
  if (sz + read_size > fsize) {
    size_t k = std::ceil((fsize - sz) / pgsize);
    read_size = std::max(k*pgsize, pgsize);
    cout << "set read size to: " << read_size << " " << (fsize - sz) << " bytes left" << endl;
  }

  while (sz + read_size < fsize) {
    void* buf = s + sz;
    cout << "reading: " << read_size << endl;
    sz += read(fd, buf, read_size);
    cout << "read: " << sz << " bytes" << endl;
    if (sz + read_size > fsize) {
      size_t k = std::ceil((fsize - sz) / pgsize);
      read_size = std::max(k*pgsize, pgsize);
      cout << "set read size to: " << read_size << " " << (fsize - sz) << " bytes left" << endl;
    }
  }
  if (sz < fsize) {
    cout << "last read: rem = " << (fsize - sz) << endl;
    void* buf = s + sz;
    sz += read(fd, buf, pgsize);
    cout << "read " << sz << " bytes " << endl;
  }
  return s;
}

auto read_compressed_graph(const char* fname, bool is_symmetric, bool mmap=false) {
  using uchar = unsigned char;
  char* s;
  size_t s_size = 0L;
  if (mmap) {
    auto SS = mmapStringFromFile(fname);
    s = SS.first;
    s_size = SS.second;
  } else {
    s = read_o_direct(fname);
  }

  long* sizes = (long*) s;
  size_t n = sizes[0], m = sizes[1];

  uintE* offsets = (uintE*) (s+3*sizeof(long));
  long skip = 3*sizeof(long) + (n+1)*sizeof(uintE);
  uintV* Degrees = (uintV*) (s+skip);
  skip += n*sizeof(uintV);
  uchar* edges = (uchar*)(s+skip);

  auto ret_offsets = pbbs::new_array_no_init<uintE>(n);
  auto ret_edges = pbbs::new_array_no_init<uintV>(m);

  parallel_for(0, n, [&] (size_t i) {
    ret_offsets[i] = Degrees[i];
  });
  auto offs = pbbs::sequence<uintE>(ret_offsets, n);
  size_t tot = pbbs::scan_inplace(offs.slice(), pbbs::addm<uintE>());
  assert(tot == m);

  parallel_for(0, n, [&] (size_t i) {
    size_t off = ret_offsets[i];
    size_t deg = ((i == (n-1)) ? m : ret_offsets[i+1]) - off;
    if (deg > 0) {
      uchar const* start = edges + offsets[i];
      uintV ngh = compression::read_first_neighbor(start, i);
      ret_edges[off] = ngh;
      if (i == 1) { cout << ngh << endl; }
      for (size_t j=1; j<deg; j++) {
        ngh += compression::read_neighbor(start);
        ret_edges[off + j] = ngh;
      }
    }
  });

  if (mmap) {
    if (munmap(s, s_size) == -1) {
      perror("munmap");
      exit(-1);
    }
  }

  return make_tuple(n, m, ret_offsets, ret_edges);
}
