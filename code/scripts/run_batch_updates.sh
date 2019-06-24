# create the data dir
mkdir -p data/batch_updates/

# make sure the binary is built
make run_batch_updates

### Experiments on small graphs
#echo "Running batch updates on soc-LJ. Data written to data/batch_updates/soc-LJ.dat"
#touch data/batch_updates/soc-LJ.dat
#numactl -i all ./run_batch_updates -s -m -f inputs/soc-LiveJournal1_sym.adj >> data/batch_updates/soc-LJ.dat
#
#echo "Running batch updates on com-orkut. Data written to data/batch_updates/com-orkut.dat"
#touch data/batch_updates/com-orkut.dat
#numactl -i all ./run_batch_updates -s -m -f inputs/com-orkut.ungraph.adj >> data/batch_updates/com-orkut.dat
#
#echo "Running batch updates on twitter_SJ. Data written to data/batch_updates/twitter_SJ.dat"
#touch data/batch_updates/twitter_SJ.dat
#numactl -i all ./run_batch_updates -s -m -f inputs/twitter_sym.adj >> data/batch_updates/twitter_SJ.dat
#
## The following are for running on large (compressed) graphs. Please uncomment
## them if you would like to run these experiments.
##

echo "Running batch updates on clueweb_sym. Data written to data/batch_updates/clueweb_sym.dat"
touch data/batch_updates/clueweb_sym.dat
numactl -i all ./run_batch_updates -s -m -c -nparts 10 -f inputs/clueweb_sym.bytepda >> data/batch_updates/clueweb_sym.dat
#
#echo "Running batch updates on hyperlink2014_sym. Data written to data/batch_updates/hyperlink2014_sym.dat"
#touch data/batch_updates/hyperlink2014_sym.dat
#numactl -i all ./run_batch_updates -s -m -c -nparts 12 -f inputs/hyperlink2014_sym.bytepda >> data/batch_updates/hyperlink2014_sym.dat

#echo "Running batch updates on hyperlink2012_sym. Data written to data/batch_updates/hyperlink2012_sym.dat"
#touch data/batch_updates/hyperlink2012_sym.dat
#numactl -i all ./run_batch_updates -s -m -c -nparts 16 -f inputs/hyperlink2012_sym.bytepda >> data/batch_updates/hyperlink2012_sym.dat
