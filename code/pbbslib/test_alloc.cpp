#include "utilities.h"
#include "sequence.h"
#include "get_time.h"
#include "list_allocator.h"
#include <vector>
using namespace pbbs;
using namespace std;

int main (int argc, char *argv[]) {
  timer t;
  //small_allocator pool;
  size_t n = 100000000;
  int rounds = 4;
  
  for (int i=0; i < rounds; i++) {
    sequence<char *> b(n, [&] (size_t i) {
	char* foo = (char*) my_alloc(48);
	foo[0] = 'a';
	foo[1] = 0;
	return foo;
      });
    t.next("allocate");
    parallel_for (0,b.size(), [&] (size_t i) {
	my_free(b[i]);
      });
    t.next("free");
  }
  cout << endl;
  //pool.print_stats();

  std::vector<int> sizes = {16, 32, 64, 128};
  small_allocator sa(sizes);
  for (int i=0; i < rounds; i++) {
    sequence<char *> b(n, [&] (size_t i) {
	char* foo = (char*) sa.alloc(64);
	foo[0] = 'a';
	foo[1] = 0;
	return foo;
      });
    t.next("small allocate");
    parallel_for (0,b.size(), [&] (size_t i) {
	sa.free(b[i], 64);
      });
    t.next("small free");
  }
  cout << endl;

  block_allocator ba(64);
  for (int j=0; j < rounds; j++) {
    sequence<char *> b(n, [&] (size_t i) {
	int l = log2_up(i);
	char* foo = (char*) ba.alloc();
	foo[0] = l;
	foo[1] = 0;
	return foo;
      });
    t.next("block allocate");
    parallel_for (0,b.size(), [&] (size_t i) {
	ba.free(b[i]);
      });
    t.next("block free");
  }
    cout << endl;


  using T = std::array<long,8>;
  using la = list_allocator<T>;
  la::init();
  //pool.print_stats();
  for (int j=0; j < rounds; j++) {
    sequence<char *> b(n, [&] (size_t i) {
	char* foo = (char*) la::alloc();
	foo[0] = 'a';
	foo[1] = 0;
	return foo;
      });
    t.next("list allocate");
    parallel_for (0,b.size(), [&] (size_t i) {
	la::free((T*) b[i]);
      });
    t.next("list free");
  }
  cout << endl;

  for (int j=0; j < rounds; j++) {
    sequence<char *> b(n, [&] (size_t i) {
	char* foo = (char*) malloc(48);
	foo[0] = 'a';
	foo[1] = 0;
	return foo;
      });
    t.next("malloc");
    parallel_for (0,b.size(), [&] (size_t i) {
	free((T*) b[i]);
      });
    t.next("list free");
  }
  cout << endl;

}
