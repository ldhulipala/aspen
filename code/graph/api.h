#pragma once

//#include "versioned_graph_waitfree.h"
#include "versioned_graph.h"
//#include "versioned_graph_2.h"
#include "../pbbslib/parse_command_line.h"
#include "../common/IO.h"
#include "../common/byte-pd-amortized.h"
//#include "other/immutable_graph.h"

using treeplus_graph = traversable_graph<sym_immutable_graph_tree_plus>;
//using simple_graph = traversable_graph<sym_immutable_graph>;

static const string default_file_name = "";

auto build_large_compressed_graph(string fname, bool is_symmetric, size_t n_parts=1) {

  auto SS = mmapStringFromFile(fname.c_str());
  char* s = SS.first;
  size_t s_size = SS.second;

  long* sizes = (long*) s;
  size_t n = sizes[0];
//  size_t m = sizes[1];
  cout << "sz[0] = " << sizes[0] << " sz[1] = " << sizes[1] << " sz[2] = " << sizes[2] << endl;

  uintE* offsets = (uintE*) (s+3*sizeof(long));
  long skip = 3*sizeof(long) + (n+1)*sizeof(uintE);
  uintV* Degrees = (uintV*) (s+skip);
  skip += n*sizeof(uintV);
  uchar* edges = (uchar*)(s+skip);

  cout << "Building compressed graph, nparts = " << n_parts << endl;

  size_t vtxs_per_batch = n/n_parts;
  size_t vertices_finished = 0;
  size_t edges_finished = 0;

  size_t bs = std::min(n-vertices_finished, vtxs_per_batch);
  pbbs::sequence<uintE> offs = pbbs::sequence<uintE>(bs);
  parallel_for (0, bs, [&] (size_t i) {
    size_t degree = Degrees[i];
    uintE v = vertices_finished + i;
    size_t actual_degree = 0;
    if (degree > 0) {
      uchar* start = edges + offsets[v];
      auto it = bytepd_amortized::simple_iter(start, degree, v);
      uintV ngh = it.cur();
      if (ngh != v) {
        actual_degree++;
      }
      while (it.has_next()) {
        uintV nxt = it.next();
        if (nxt != ngh || nxt != v) {
          actual_degree++;
        }
        ngh = nxt;
      }
    }
    offs[i] = actual_degree;
  }, 1);
  uintE n_edges = pbbs::scan_inplace(offs.slice(), pbbs::addm<uintE>());
  cout << "Next batch num edges = " << n_edges << endl;
  edges_finished += n_edges;

  auto init_batch_edges = pbbs::new_array_no_init<uintV>(n_edges);
  parallel_for(0, bs, [&] (size_t i) {
    size_t off = offs[i];
    size_t original_degree = Degrees[i];
    size_t degree = ((i == (bs-1)) ? (n_edges) : offs[i+1]) - off;
    if (degree > 0) {
      uintE v = vertices_finished + i;
      uchar* start = edges + offsets[v];
      auto it = bytepd_amortized::simple_iter(start, original_degree, v);
      uintV ngh = it.cur();
      size_t k = 0;
      if (ngh != v) {
        init_batch_edges[off] = ngh;
        k++;
      }
      while (it.has_next()) {
        uintV nxt = it.next();
        if (nxt != ngh || nxt != v) {
          init_batch_edges[off + k++] = nxt;
        }
        ngh = nxt;
      }
      assert(k == degree);
    }
  }, 1);

  cout << "Inserting next batch, bs = " << bs << endl;
  // Treeplus graph handles freeing offs and the edges.
  auto G = treeplus_graph(bs, n_edges, offs.to_array(), init_batch_edges);
  debug(cout << "Deleting" << endl;);
  debug(cout << "Done!" << endl;);
  G.print_stats();
  cout << "vertices remaining: " << (n - vertices_finished) << endl;
  vertices_finished += bs;

  while (vertices_finished < n) {
    size_t bs = std::min(n-vertices_finished, vtxs_per_batch);
    offs = pbbs::sequence<uintE>(bs);
    parallel_for (0, bs, [&] (size_t i) {
      uintE v = vertices_finished + i;
      size_t degree = Degrees[v];
      size_t actual_degree = 0;
      if (degree > 0) {
        uchar* start = edges + offsets[v];
        auto it = bytepd_amortized::simple_iter(start, degree, v);
        uintV ngh = it.cur();
        if (ngh != v) {
          actual_degree++;
        }
        while (it.has_next()) {
          uintV nxt = it.next();
          if (nxt != ngh || nxt != v) {
            actual_degree++;
          }
          ngh = nxt;
        }
      }
      offs[i] = actual_degree;
//      assert(actual_degree == degree);
    }, 1);
    n_edges = pbbs::scan_inplace(offs.slice(), pbbs::addm<uintE>());
    debug(cout << "Next batch num edges = " << n_edges << endl;);
    edges_finished += n_edges;

    auto batch_edges = pbbs::new_array_no_init<uintV>(n_edges);
    parallel_for(0, bs, [&] (size_t i) {
      size_t off = offs[i];
      size_t original_degree = Degrees[vertices_finished + i];
      size_t degree = ((i == (bs-1)) ? (n_edges) : offs[i+1]) - off;
      if (degree > 0) {
        uintE v = vertices_finished + i;
        uchar* start = edges + offsets[v];
        auto it = bytepd_amortized::simple_iter(start, original_degree, v);
        uintV ngh = it.cur();
        size_t k = 0;
        if (ngh != v) {
          batch_edges[off] = ngh;
          k++;
        }
        while (it.has_next()) {
          uintV nxt = it.next();
          if (nxt != ngh || nxt != v) {
            batch_edges[off + k++] = nxt;
          }
          ngh = nxt;
        }
//        assert(k == degree);
      }
    }, 1);

    debug(cout << endl;
    cout << "Inserting next batch, bs = " << bs << endl;);
    // TODO: check who deletes edges and offs.
    auto G_next = G.insert_edges_batch(bs, n_edges, offs.to_array(), batch_edges, vertices_finished);
    debug(cout << "Done!" << endl;);
    G.print_stats();
    G = G_next;
    debug(cout << "vertices remaining: " << (n - vertices_finished) << endl;);
    vertices_finished += bs;
  }
  cout << "Inserted n = " << vertices_finished << " vertices and m = " << edges_finished << " edges " << endl;

  // Munmap the mmap'd file.
  if (munmap(s, s_size) == -1) {
    perror("munmap");
    exit(-1);
  }

  return versioned_graph<treeplus_graph>(std::move(G));
}

auto initialize_graph(string fname="", bool mmap=false, bool is_symmetric=true, bool compressed=false, size_t n_parts=1) {
  if (fname == "") {
    cout << "Unimplemented!" << endl;
    exit(0);
  }
  if (compressed && n_parts > 1) {
    cout << "Building large compressed graph" << endl;
    return build_large_compressed_graph(fname, is_symmetric, n_parts);
  }
  size_t n; size_t m;
  uintE* offsets; uintV* edges;
  if (!compressed) {
    cout << "Reading Unweighted Graph" << endl;
    std::tie(n, m, offsets, edges) = read_unweighted_graph(fname.c_str(), is_symmetric, mmap);
    cout << "Read Unweighted Graph" << endl;
  } else {
    cout << "Reading Compressed Graph" << endl;
    std::tie(n, m, offsets, edges) = read_compressed_graph(fname.c_str(), is_symmetric, mmap);
    cout << "Read Compressed Graph" << endl;
  }
  return versioned_graph<treeplus_graph>(n, m, offsets, edges);
}


auto empty_treeplus_graph() {
  return versioned_graph<treeplus_graph>();
}

auto initialize_treeplus_graph(commandLine& P) {
  string fname = string(P.getOptionValue("-f", default_file_name.c_str()));
  bool mmap = P.getOption("-m");
  bool is_symmetric = P.getOption("-s");
  bool compressed = P.getOption("-c");
  size_t n_parts = P.getOptionLongValue("-nparts", 1);

  return initialize_graph(fname, mmap, is_symmetric, compressed, n_parts);
}

auto get_graph_edges(const char* fname, bool is_symmetric, bool mmap=false) {
  auto T = read_unweighted_graph(fname, is_symmetric, mmap);
  size_t n = get<0>(T); size_t m = get<1>(T);
  uintE* offsets = get<2>(T); uintV* edges = get<3>(T);
  return make_tuple(n, m, offsets, edges);
}
