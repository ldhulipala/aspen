# create the data dir
mkdir -p data/simultaneous_updates_queries/

# make sure the binary is built
make run_simultaneous_updates_queries

echo "Running simultaneous queries and updates on soc-LJ.\n"
echo "Data will bewritten to data/simultaneous_updates_queries/socLJ.dat.\n"
touch data/simultaneous_updates_queries/socLJ.dat
echo "Running simultaneous queries and updates on soc-LJ\n" > data/simultaneous_updates_queries/socLJ.dat
numactl -i all ./run_simultaneous_updates_queries -queryiters 200 -m -s -f inputs/soc-LiveJournal1_sym.adj >> data/simultaneous_updates_queries/socLJ.dat
echo "\nRunning just queries on soc-LJ\n" >> data/simultaneous_updates_queries/socLJ.dat
numactl -i all ./run_simultaneous_updates_queries -noupdate -queryiters 200 -m -s -f inputs/soc-LiveJournal1_sym.adj >> data/simultaneous_updates_queries/socLJ.dat
echo "\nRunning just updates on soc-LJ\n" >> data/simultaneous_updates_queries/socLJ.dat
numactl -i all ./run_simultaneous_updates_queries -noquery -m -s -f inputs/soc-LiveJournal1_sym.adj >> data/simultaneous_updates_queries/socLJ.dat

echo "Running simultaneous queries and updates on com-orkut.\n"
echo "Data will be written to data/simultaneous_updates_queries/com-orkut.dat.\n"
touch data/simultaneous_updates_queries/com-orkut.dat
echo "Running simultaneous queries and updates on com-orkut\n" > data/simultaneous_updates_queries/com-orkut.dat
numactl -i all ./run_simultaneous_updates_queries -queryiters 200 -m -s -f inputs/com-orkut.adj >> data/simultaneous_updates_queries/com-orkut.dat
echo "\nRunning just queries on com-orkut\n" >> data/simultaneous_updates_queries/com-orkut.dat
numactl -i all ./run_simultaneous_updates_queries -noupdate -queryiters 200 -m -s -f inputs/com-orkut.adj >> data/simultaneous_updates_queries/com-orkut.dat
echo "\nRunning just updates on com-orkut\n" >> data/simultaneous_updates_queries/com-orkut.dat
numactl -i all ./run_simultaneous_updates_queries -noquery -m -s -f inputs/com-orkut.adj >> data/simultaneous_updates_queries/com-orkut.dat

echo "Running simultaneous queries and updates on twitter_SJ\n"
echo "Data will be written to data/simultaneous_updates_queries/twitter_SJ.dat.\n"
touch data/simultaneous_updates_queries/twitter_SJ.dat
echo "Running simultaneous queries and updates on twitter_SJ.adj\n" > data/simultaneous_updates_queries/twitter_SJ.dat
numactl -i all ./run_simultaneous_updates_queries -queryiters 100 -m -s -f inputs/twitter_SJ.adj >> data/simultaneous_updates_queries/twitter_SJ.dat
echo "\nRunning just queries on twitter_SJ\n" >> data/simultaneous_updates_queries/twitter_SJ.dat
numactl -i all ./run_simultaneous_updates_queries -noupdate -queryiters 100 -m -s -f inputs/twitter_SJ.adj >> data/simultaneous_updates_queries/twitter_SJ.dat
echo "\nRunning just updates on twitter_SJ\n" >> data/simultaneous_updates_queries/twitter_SJ.dat
numactl -i all ./run_simultaneous_updates_queries -noquery -m -s -f inputs/twitter_SJ.adj >> data/simultaneous_updates_queries/twitter_SJ.dat


# The following are for running on large (compressed) graphs. Please uncomment
# them if you would like to run these experiments.
#
# echo "Running simultaneous queries and updates on clueweb_sym\n"
# echo "Data will be written to data/simultaneous_updates_queries/clueweb.dat.\n"
# touch data/simultaneous_updates_queries/clueweb.dat
# echo "Running simultaneous queries and updates on clueweb_sym\n" > data/simultaneous_updates_queries/clueweb.dat
# numactl -i all ./run_simultaneous_updates_queries -queryiters 30 -m -s -c -nparts 8 -f inputs/clueweb_sym.bytepda >> data/simultaneous_updates_queries/clueweb.dat
# echo "\nRunning just queries on clueweb_sym\n" >> data/simultaneous_updates_queries/clueweb.dat
# numactl -i all ./run_simultaneous_updates_queries -noupdate -queryiters 30 -m -s -c -nparts 8 -f inputs/clueweb_sym.bytepda >> data/simultaneous_updates_queries/clueweb.dat
# echo "\nRunning just updates on clueweb_sym\n" >> data/simultaneous_updates_queries/clueweb.dat
# numactl -i all ./run_simultaneous_updates_queries -noquery -m -s -c -nparts 8  -f inputs/clueweb_sym.bytepda >> data/simultaneous_updates_queries/clueweb.dat
#
# echo "Running simultaneous queries and updates on hyperlink2014_sym\n"
# echo "Data will be written to data/simultaneous_updates_queries/hyperlink2014_sym.dat.\n"
# touch data/simultaneous_updates_queries/hyperlink2014_sym.dat
# echo "Running simultaneous queries and updates on hyperlink2014_sym\n" > data/simultaneous_updates_queries/hyperlink2014_sym.dat
# numactl -i all ./run_simultaneous_updates_queries -queryiters 30 -m -s -c -nparts 8 -f inputs/hyperlink2014_sym.bytepda >> data/simultaneous_updates_queries/hyperlink2014_sym.dat
# echo "\nRunning just queries on hyperlink2014_sym\n" >> data/simultaneous_updates_queries/hyperlink2014_sym.dat
# numactl -i all ./run_simultaneous_updates_queries -noupdate -queryiters 30 -m -s -c -nparts 8 -f inputs/hyperlink2014_sym.bytepda >> data/simultaneous_updates_queries/hyperlink2014_sym.dat
# echo "\nRunning just updates on hyperlink2014_sym\n" >> data/simultaneous_updates_queries/hyperlink2014_sym.dat
# numactl -i all ./run_simultaneous_updates_queries -noquery -m -s -c -nparts 8  -f inputs/hyperlink2014_sym.bytepda >> data/simultaneous_updates_queries/hyperlink2014_sym.dat
#
# echo "Running simultaneous queries and updates on hyperlink2012_sym\n"
# echo "Data will be written to data/simultaneous_updates_queries/hyperlink2012_sym.dat.\n"
# touch data/simultaneous_updates_queries/hyperlink2012_sym.dat
# echo "Running simultaneous queries and updates on hyperlink2012_sym\n" > data/simultaneous_updates_queries/hyperlink2012_sym.dat
# numactl -i all ./run_simultaneous_updates_queries -queryiters 30 -m -s -c -nparts 8 -f inputs/hyperlink2012_sym.bytepda >> data/simultaneous_updates_queries/hyperlink2012_sym.dat
# echo "\nRunning just queries on hyperlink2012_sym\n" >> data/simultaneous_updates_queries/hyperlink2012_sym.dat
# numactl -i all ./run_simultaneous_updates_queries -noupdate -queryiters 30 -m -s -c -nparts 8 -f inputs/hyperlink2012_sym.bytepda >> data/simultaneous_updates_queries/hyperlink2012_sym.dat
# echo "\nRunning just updates on hyperlink2012_sym\n" >> data/simultaneous_updates_queries/hyperlink2012_sym.dat
# numactl -i all ./run_simultaneous_updates_queries -noquery -m -s -c -nparts 8  -f inputs/hyperlink2012_sym.bytepda >> data/simultaneous_updates_queries/hyperlink2012_sym.dat
