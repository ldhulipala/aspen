#include "sequence.h"
#include "random.h"
#include "get_time.h"
#include "strings/string_basics.h"
#include "parse_command_line.h"

using namespace std;
using namespace pbbs;

/*
template <class T, class intType>
struct nested_sequence {
  using value_type = range<T*>;
  sequence<T> values;
  sequence<intType> offsets;

  nested_sequence(sequence<T> a, sequence<intType> offsets) values(a), offsets(offsets) {}

  value_type& operator[] (const size_t i) const {
    return values.slice(offsets[i],offsets[i+1]);
  }
};
		  
template <class K, class V, class Comp>
sequence<K,range<V>> group_by(sequence<pair<K,V>> pairs, Comp less) {
  using KV = pair<K,V>;
  timer t("group_by",true);
  auto kless = [&] (KV a, KV b) {
    return a.first < b.first;}
  sequence<pair<K,V>> sorted = pbbs::sort(pairs, kless);  
  t.next("sort");
  pbbs::sequence<bool> Fl(n, [&] (size_t i) {
	return (i==0) || kless(sorted[i-1], sorted[i]) == -1;});
  t.next("mark");

  auto parts = pbbs::partition_at(words, Fl);
  t.next("partition");
}
*/

sequence<range<char*>> split(sequence<char> const &str, std::string const &foo) {
  auto f = [&] (char a) {
    for (int i = 0 ; i < foo.size(); i ++)
      if (a == foo[i]) return true;
    return false;
  };
  return pbbs::tokens(str, f);
}

int main (int argc, char *argv[]) {
  commandLine P(argc, argv, "[-r <rounds>] filename");
  int rounds = P.getOptionIntValue("-r", 3);
  char* filename = P.getArgument(0);
  timer t("word counts", true);

  sequence<char> translate(256, [&] (size_t i) {return ' ';});
  for (char i='a' ; i <= 'z'; i++) translate[i] = i;
  for (char i='A' ; i <= 'Z'; i++) translate[i] = i + 32;
  string keep = " \n\t\r";
  for (int i=0; i < keep.size(); i++)
    translate[keep[i]] = keep[i];
  
  pbbs::sequence<char> str = pbbs::char_seq_from_file(filename);
  t.next("read file");
  std::string a("\n\r");
  std::string b(" ");
  auto is_space = [&] (char a) {return a == ' ' || a == '\t';};

  using kvpair = std::pair<char*, size_t>;
  
  for (int i=0; i < rounds ; i++) {
    sequence<char> str2(str.size(), [&] (size_t i) {
      return translate[str[i]];});
    t.next("translate");

    auto is_line = [&] (char a) {return a == '\n' || a == '\r';};
    //pbbs::sequence<range<char*>> lines = tokens(str, is_line);
    pbbs::sequence<range<char*>> lines = split(str2, a);
    t.next("tokenize");
    size_t n = lines.size();
    cout << n << endl;
    sequence<sequence<kvpair>> foo(n, [&] (size_t i) {
	auto words = tokenize(lines[i], is_space);
	return sequence<kvpair>(words.size(), [&] (size_t j) {
	    return kvpair(words[j], i);
	  });
      });
    t.next("allocate");

    sequence<kvpair> bar = flatten(foo);
    t.next("flatten");
    cout << bar.size() << endl;

    auto kless = [&] (kvpair a, kvpair b) {
      return strcmp(a.first, b.first) < 0;};
    
    sequence<kvpair> sorted = pbbs::sort(bar, kless);  
    t.next("sort");
    
    pbbs::sequence<bool> Fl(bar.size(), [&] (size_t i) {
	return (i==0) || kless(sorted[i-1], sorted[i]);});
    t.next("mark");

    auto idx = pack_index<size_t>(Fl);
    t.next("pack index");
    sequence<pair<char*,sequence<size_t>>> r(idx.size(), [&] (size_t i) {
	size_t m = ((i==idx.size()-1) ? bar.size() : idx[i+1]) - idx[i];
	return make_pair(sorted[idx[i]].first,
			 sequence<size_t>(m, [&] (size_t j) {
			     return sorted[idx[i] + j].second;}));});
    //auto parts = pbbs::partition_at(sorted, Fl);
    t.next("partition");
    
    foo.clear();
    t.next("free");
  }
  
}
