# create the data dir
mkdir -p data/static_algorithms/nibble/

# make sure the binary is built
make run_static_algorithm

### Experiments on small graphs
echo "Running Nibble queries on soc-LJ. Data written to data/static_algorithms/nibble/soc-LJ.dat"
touch data/static_algorithms/nibble/soc-LJ.dat
numactl -i all ./run_static_algorithm -BS 144 -t NIBBLE -nsrc 1024 -s -m -f inputs/soc-LiveJournal1_sym.adj >> data/static_algorithms/nibble/soc-LJ.dat

echo "Running Nibble queries on com-orkut. Data written to data/static_algorithms/nibble/com-orkut.dat"
touch data/static_algorithms/nibble/com-orkut.dat
numactl -i all ./run_static_algorithm -BS 144 -t NIBBLE -nsrc 1024 -s -m -f inputs/com-orkut.adj >> data/static_algorithms/nibble/com-orkut.dat

echo "Running Nibble queries on twitter_SJ. Data written to data/static_algorithms/nibble/twitter_SJ.dat"
touch data/static_algorithms/nibble/twitter_SJ.dat
numactl -i all ./run_static_algorithm -BS 144 -t NIBBLE -nsrc 1024 -s -m -f inputs/twitter_SJ.adj >> data/static_algorithms/nibble/twitter_SJ.dat


# The following are for running on large (compressed) graphs. Please uncomment
# them if you would like to run these experiments.
#
#echo "Running Nibble queries on clueweb_sym. Data written to data/static_algorithms/nibble/clueweb_sym.dat"
#touch data/static_algorithms/nibble/clueweb_sym.dat
#numactl -i all ./run_static_algorithm -BS 144 -t NIBBLE -nsrc 1024 -s -m -c -nparts 8 -f inputs/clueweb_sym.bytepda >> data/static_algorithms/nibble/clueweb_sym.dat
#
#echo "Running Nibble queries on hyperlink2014_sym. Data written to data/static_algorithms/nibble/hyperlink2014_sym.dat"
#touch data/static_algorithms/nibble/hyperlink2014_sym.dat
#numactl -i all ./run_static_algorithm -BS 144 -t NIBBLE -nsrc 1024 -s -m -c nparts 10 -f inputs/hyperlink2014_sym.bytepda >> data/static_algorithms/nibble/hyperlink2014_sym.dat
#
#echo "Running Nibble queries on hyperlink2012_sym. Data written to data/static_algorithms/nibble/hyperlink2012_sym.dat"
#touch data/static_algorithms/nibble/hyperlink2012_sym.dat
#numactl -i all ./run_static_algorithm -BS 144 -t NIBBLE -nsrc 1024 -s -m -c -nparts 16 -f inputs/hyperlink2012_sym.bytepda >> data/static_algorithms/nibble/hyperlink2012_sym.dat
