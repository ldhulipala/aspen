#define HOMEGROWN
#define CUSTOMPOOL

#include "../graph/api.h"
#include "../trees/utils.h"
#include "../algorithms/BFS.h"

#include "../lib_extensions/sparse_table_hash.h"
#include "../pbbslib/random_shuffle.h"

#include <cstring>
#include <vector>
#include <algorithm>
#include <chrono>
#include <thread>
#include <cmath>
#include <cmath>
#include <random>
#include <unistd.h>
#include <iostream>
#include <fstream>

using namespace std;
using edge_seq = pair<uintV, uintV>;

// bool = true if the update is an insertion
using update = pair<edge_seq, bool>;

void write_datapoints_to_csv(std::string& fname, pbbs::sequence<pair<double, double>> const& S, size_t n_points) {
  double min_t = S[0].first;
  double max_t = S[S.size()-1].first;

  cout << "min_t = " << min_t << " max_t = " << max_t << " nm data points = " << S.size() << endl;

  auto output_pts = pbbs::sequence<pair<double, double>>(n_points);
  double window_size = (max_t - min_t) / n_points;

  size_t k = 0;
  double window_start = min_t;
  double window_end = min_t + window_size;
  size_t pts_in_last_window = 0;
  double total_in_last_window = 0.0;

  std::vector<double> pts_in_window;

  for (size_t i=0; i<S.size(); i++) {
    double t_i = S[i].first;
    double lat_i = S[i].second;
    if (t_i < window_end) {
      pts_in_last_window++;
      total_in_last_window += lat_i;
      pts_in_window.push_back(lat_i);
    } else {
      if (pts_in_last_window) {
        double avg_latency = total_in_last_window / pts_in_last_window;
        output_pts[k++] = make_pair((window_start + window_end) / 2, avg_latency);
      }
      window_start = window_end;
      window_end += window_size;
      pts_in_window.clear();

      pts_in_window.push_back(lat_i);
    }
  }

  ofstream outf;
  outf.open(fname);
  outf << "time, latency" << endl;
  for (size_t i=0; i<k; i++) {
    outf <<  fixed << setprecision(9) << output_pts[i].first << "," << output_pts[i].second << endl;
  }
  outf.close();
}

void sequential_update_parallel_query(commandLine& P) {

  bool no_update = P.getOptionValue("-noupdate");
  bool no_query = P.getOptionValue("-noquery");

  size_t n_query_threads = P.getOptionLongValue("-query_threads", std::thread::hardware_concurrency()-1);

  auto query_scheduler = fork_join_scheduler(n_query_threads, /* start offset */0, /* include_self */true, /* set affinity */ false);

  string update_fname = P.getOptionValue("-update-file", "updates.dat");
  string query_fname = P.getOptionValue("-query-file", "queries.dat");

  versioned_graph<treeplus_graph> VG = initialize_treeplus_graph(P);

// The following may be needed if you are running on large graphs and tight on
// memory.
//  int ok;
//  cout << "Enter after dropping caches" << endl;
//  cin >> ok;
  double beta = 0.01;
  double gamma = 0.1;

  // 1. iterate over all edges, and capture a beta-fraction.
  auto S = VG.acquire_version();
  const auto& GA = S.graph;
  size_t n = GA.num_vertices();
  size_t m = GA.num_edges();
  size_t sample_size = P.getOptionLongValue("-samplesize", 4000000);
  size_t updates_to_run = P.getOptionLongValue("-updatestorun", 2000000);
  beta = (1.0*sample_size) / m;
  auto _empty = make_tuple(make_pair(UINT_V_MAX, UINT_V_MAX), pbbs::empty());

  size_t up_m = 1L << ((size_t)pbbs::log2_up<size_t>(m));
  size_t mask = up_m - 1;
  size_t threshold = ceil(beta*up_m);

  auto hash_fn = [&] (const pair<uintV, uintV>& k) {
    return pbbs::hash64(k.first) ^ pbbs::hash64(k.second);
  };

  auto tab = make_sparse_table_hash<pair<uintV, uintV>, pbbs::empty>(20000000, _empty, hash_fn);

  timer map_t; map_t.start();
  auto r = pbbs::random();
  auto map_f = [&] (const uintV& u, const uintV& v) {
    size_t combined_value = (size_t(uint32_t(u)) << 32L) | size_t(uint32_t(v));
    size_t hsh = pbbs::hash64(combined_value) & mask;
    if (hsh < threshold) {
      tab.insert(make_tuple(make_pair(std::min(u, v), std::max(u, v)), pbbs::empty()));
    }
  };
  GA.map_all_edges(map_f);
  VG.release_version(std::move(S));

  auto sample = tab.entries();
  tab.del();
  // cout << "sample size: " << sample.size() << endl;
  assert(sample.size() > 0);

  r = r.next();
  size_t up_sample = 1 << pbbs::log2_up(sample.size());
  size_t deletion_mask = up_sample - 1;
  size_t deletion_threshold = gamma * up_sample;
  auto should_delete = [&] (const tuple<pair<uintV, uintV>, pbbs::empty>& A) {
    auto u = get<0>(A).first; auto v = get<0>(A).second;
    auto hsh = (r.ith_rand(u) ^ r.ith_rand(v)) & deletion_mask;
    return hsh < deletion_threshold;
  };
  auto should_insert = [&] (const tuple<pair<uintV, uintV>, pbbs::empty>& A) {
    auto u = get<0>(A).first; auto v = get<0>(A).second;
    auto hsh = (r.ith_rand(u) ^ r.ith_rand(v)) & deletion_mask;
    return !(hsh < deletion_threshold);
  };

  auto deletes = pbbs::filter(sample, should_delete);
  auto inserts = pbbs::filter(sample, should_insert);
  sample.clear();

  using pair_vertex = tuple<uintV, uintV>;
  size_t n_inserts = inserts.size();
  auto immediate_deletions = pbbs::new_array_no_init<pair_vertex>(2*n_inserts);
  for(size_t i=0; i<n_inserts; i++) {
    immediate_deletions[2*i] = make_tuple(get<0>(inserts[i]).first, get<0>(inserts[i]).second);
    immediate_deletions[2*i+1] = make_tuple(get<0>(inserts[i]).second, get<0>(inserts[i]).first);
  }

  VG.delete_edges_batch(2*n_inserts, immediate_deletions, false, true);

  // auto S2 = VG.acquire_version();
  // S2.graph.check_edges();
  // S2.graph.size_in_bytes();
  // cout << "Deleted insertion updates, |E| is now: " << S2.graph.num_edges() << endl;
  // VG.release_version(std::move(S2));

  // 2. Generate the sequence of insertions and deletions
  size_t num_updates = inserts.size() + deletes.size();
  auto updates = pbbs::sequence<update>(inserts.size() + deletes.size());
  parallel_for(0, deletes.size(), [&] (size_t i) {
    updates[i] = make_pair(make_pair(get<0>(deletes[i]).first, get<0>(deletes[i]).second), false);
  });
  size_t n_deletes = deletes.size();
  parallel_for(0, inserts.size(), [&] (size_t i) {
    updates[n_deletes + i] = make_pair(make_pair(get<0>(inserts[i]).first, get<0>(inserts[i]).second), true);
  });

  cout << "Shuffling updates" << endl;
  cout << "n_deletes = " << n_deletes << " n_inserts = " << inserts.size() << endl;
  auto the_updates = pbbs::random_shuffle(updates);

  // 3. Launch parallel queries and sequential updates


  //  auto update_times = pbbs::sequence<pair<double, double>>(num_updates_to_apply);
  size_t num_updates_to_apply = std::min(num_updates, (size_t)updates_to_run);
  size_t update_start = 0;
  size_t update_end = num_updates_to_apply;


  bool updates_finished = false;

  timer update_timer; update_timer.start();
  std::function<void()> updater = [&, no_update, update_start, update_end] () {
    cout << "Started updates" << endl;
    if (!no_update) {
      timer ut; ut.start();
      auto next_batch = pbbs::new_array_no_init<pair_vertex>(2);
      double total_update_time = 0.0;
      size_t start = update_start;
      while (start < update_end) {
        const auto& next_update = the_updates[start++];
        uintV u = std::min(next_update.first.second, next_update.first.first);
        uintV v = std::max(next_update.first.second, next_update.first.first);
        next_batch[0] = make_pair(u, v);
        next_batch[1] = make_pair(v, u);

        if (next_update.second) { // insertion
          VG.insert_edges_batch(2, next_batch, /*sorted=*/true, /*remove_dups=*/false, n, /*run_seq=*/true);
        } else { // deletion
          VG.delete_edges_batch(2, next_batch, /*sorted=*/true, /*remove_dups=*/false, n, /*run_seq=*/true);
        }
      }
      total_update_time = ut.stop();
      pbbs::free_array(next_batch);

      cout << "Update throughput = " << (updates_to_run / total_update_time) << endl;
      cout << "Average_latency = " << (total_update_time / updates_to_run) << endl;
      cout << "Total update time = " << total_update_time << endl;

      // to generate latency plots
      // write_datapoints_to_csv(update_fname, update_times, 1000);
    }
    updates_finished = true;
  };

  size_t query_iters = P.getOptionLongValue("-queryiters", 100);

  std::default_random_engine generator;
  std::poisson_distribution<int> distribution(300); // mean is 300ms

  std::function<void()> query = [&] () {
    cout << "Started queries" << endl;
    if (!no_query) {
      double total_query_time = 0.0;
      auto rr = pbbs::random();
      timer query_timer; query_timer.start();
      size_t iters=0;
      if (P.getOptionValue("-bounded")) {
        for (size_t i=0; i<query_iters; i++) {
          auto S = VG.acquire_version();

          double start_time = update_timer.get_time();
          size_t src = rr.ith_rand(0) % n;
          rr = rr.next();
          BFS_Fetch(S.graph, src);
          double end_time = update_timer.get_time();
          double query_time = end_time - start_time;
          cout << "query time = " << query_time << endl;

          // query_times[i] = make_pair(end_time - experiment_start, query_time);
          total_query_time += query_time;

          VG.release_version(std::move(S));
          iters++;
        }
      } else {
        while (!updates_finished) {
          auto S = VG.acquire_version();

          double start_time = update_timer.get_time();
          size_t src = rr.ith_rand(0) % n;
          rr = rr.next();
          BFS_Fetch(S.graph, src);
          double end_time = update_timer.get_time();
          double query_time = end_time - start_time;
          cout << "query time = " << query_time << endl;

          // query_times[i] = make_pair(end_time - experiment_start, query_time);
          total_query_time += query_time;

          VG.release_version(std::move(S));
          iters++;

          // Full load is unrealistic; sample sleeps from a poisson
          // distribution.
          int poisson_sleep = distribution(generator);
          std::this_thread::sleep_for(std::chrono::milliseconds(poisson_sleep));
        }
      }
      cout << "Average query time : " << (total_query_time / iters) << endl;
    }
    return true;
  };

//  tree_parallel=false;
  cout << "SeqUpdateParQuery:" << endl;
  auto update_scheduler = fork_join_scheduler(1, /* start offset */n_query_threads, /* include_self */false, /* set_affinity */ true);
  cout << "Created update scheduler:" << endl;

  update_scheduler.sched->send(&updater, 0);

  query();

  while (!updates_finished) {
    std::this_thread::yield();
  }
}

int main(int argc, char** argv) {
//  cout << "Running with " << num_workers() << " threads" << endl;
  commandLine P(argc, argv, "./test_graph [-f file -m (mmap) <testid>]");
//  create_star(P);
  sequential_update_parallel_query(P);
}
