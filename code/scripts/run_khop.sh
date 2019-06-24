# create the data dir
mkdir -p data/static_algorithms/khop/

# make sure the binary is built
make run_static_algorithm

### Experiments on small graphs
echo "Running kHop queries on soc-LJ. Data written to data/static_algorithms/khop/soc-LJ.dat"
touch data/static_algorithms/khop/soc-LJ.dat
numactl -i all ./run_static_algorithm -BS 144 -t KHOP -nsrc 500 -s -m -f inputs/soc-LiveJournal1_sym.adj >> data/static_algorithms/khop/soc-LJ.dat

echo "Running kHop queries on com-orkut. Data written to data/static_algorithms/khop/com-orkut.dat"
touch data/static_algorithms/khop/com-orkut.dat
numactl -i all ./run_static_algorithm -BS 144 -t KHOP -nsrc 500 -s -m -f inputs/com-orkut.adj >> data/static_algorithms/khop/com-orkut.dat

echo "Running kHop queries on twitter_SJ. Data written to data/static_algorithms/khop/twitter_SJ.dat"
touch data/static_algorithms/khop/twitter_SJ.dat
numactl -i all ./run_static_algorithm -BS 144 -t KHOP -nsrc 200 -s -m -f inputs/twitter_SJ.adj >> data/static_algorithms/khop/twitter_SJ.dat


# The following are for running on large (compressed) graphs. Please uncomment
# them if you would like to run these experiments.
#
#echo "Running kHop queries on clueweb_sym. Data written to data/static_algorithms/khop/clueweb_sym.dat"
#touch data/static_algorithms/khop/clueweb_sym.dat
#numactl -i all ./run_static_algorithm -BS 40 -t KHOP -nsrc 120 -s -m -c -nparts 8 -f inputs/clueweb_sym.bytepda >> data/static_algorithms/khop/clueweb_sym.dat
#
#echo "Running kHop queries on hyperlink2014_sym. Data written to data/static_algorithms/khop/hyperlink2014_sym.dat"
#touch data/static_algorithms/khop/hyperlink2014_sym.dat
#numactl -i all ./run_static_algorithm -BS 40 -t KHOP -nsrc 120 -s -m -c nparts 10 -f inputs/hyperlink2014_sym.bytepda >> data/static_algorithms/khop/hyperlink2014_sym.dat
#
#echo "Running kHop queries on hyperlink2012_sym. Data written to data/static_algorithms/khop/hyperlink2012_sym.dat"
#touch data/static_algorithms/khop/hyperlink2012_sym.dat
#numactl -i all ./run_static_algorithm -BS 40 -t KHOP -nsrc 120 -s -m -c -nparts 16 -f inputs/hyperlink2012_sym.bytepda >> data/static_algorithms/khop/hyperlink2012_sym.dat
