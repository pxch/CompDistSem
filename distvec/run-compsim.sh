#!/bin/bash
#----------------------------------------------------
# Example SLURM job script to run OpenMP applications
# on TACC's Stampede system.
#----------------------------------------------------
#SBATCH -J distvec_comp_phrasal_sim       # Job name
#SBATCH -o distvec_comp_phrasal_sim.o%j   # Name of stdout output file(%j expands to jobId)
#SBATCH -e distvec_comp_phrasal_sim.o%j   # Name of stderr output file(%j expands to jobId)
#SBATCH -p gpu              # Submit to the 'normal' or 'development' queue
#SBATCH -N 1                # Total number of nodes requested (16 cores/node)
#SBATCH -n 1                # Total number of mpi tasks requested
#SBATCH -t 02:00:00         # Run time (hh:mm:ss) - 1.5 hours
# The next line is required if the user has more than one project
# #SBATCH -A UT-DEFT  # Allocation name to charge job against

# This example will run an OpenMP application using 16 threads

# Set the number of threads per task(Default=1)
export OMP_NUM_THREADS=16

# Run the OpenMP application
make compsim
#./compsim 100 20000 ../corpora/ukwac_2/wordvocab/ word.dm mod_mat_2/ ../parseRule/output_3-rule ph-sim.txt
#./compsim 300 50000 ../corpora/ukwac_1_50k_20k_2/wordvocab/ mod_mat_50k_20k_300_1/word.dm mod_mat_50k_20k_300_1/ ../parseRule/output_new_1-rule ph-sim_50k_20k_300.txt
#./compsim 300 50000 ../corpora/ukwac_1_50k_20k_2/wordvocab/ mod_mat_50k_20k_300_1/word.dm mod_mat_50k_20k_300_1/ ../parseRule/output_new_3_train-rule ph-sim_50k_20k_300_3_train.txt
#./compsim 300 50000 ../corpora/ukwac_1_50k_20k_2/wordvocab/ mod_mat_50k_20k_300_1/word.dm mod_mat_50k_20k_300_1/ ../parseRule/output_new_3_test-rule ph-sim_50k_20k_300_3_test.txt
#./compsim 300 50000 ../corpora/ukwac_1_50k_20k_2/wordvocab/ mod_mat_50k_20k_300_1/word.dm mod_mat_50k_20k_300_1/ ../parseRule/output_new_3_train-rule ph-sim_50k_20k_300_3_1_train
#./compsim 300 50000 ../corpora/ukwac_1_50k_20k_2/wordvocab/ mod_mat_50k_20k_300_1/word.dm mod_mat_50k_20k_300_1/ ../parseRule/output_new_3_test-rule ph-sim_50k_20k_300_3_1_test
#./compsim 300 50000 ../../corpora/ukwac_1_50k_20k_2/wordvocab/ ../data/mod_mat_50k_20k_300_1/word.dm ../data/mod_mat_50k_20k_300_1/ ../data/rules/final/sick_1_train-rule ../data/phsim/final/sick_1_train
#./compsim 300 50000 ../../corpora/ukwac_1_50k_20k_2/wordvocab/ ../data/mod_mat_50k_20k_300_1/word.dm ../data/mod_mat_50k_20k_300_1/ ../data/rules/final/sick_1_test-rule ../data/phsim/final/sick_1_test
./compsim 300 50000 ../../corpora/bnc+ukwac+wiki/wordvocab/ ../data/mod_mat_bnc+ukwac+wiki/ ../data/rules/0407/sick_1_new_2_train-rule ../data/phsim/0408/sick_1_new_2_train
./compsim 300 50000 ../../corpora/bnc+ukwac+wiki/wordvocab/ ../data/mod_mat_bnc+ukwac+wiki/ ../data/rules/0407/sick_1_new_2_test-rule ../data/phsim/0408/sick_1_new_2_test
./compsim 300 50000 ../../corpora/bnc+ukwac+wiki/wordvocab/ ../data/mod_mat_bnc+ukwac+wiki/ ../data/rules/0407/rte1_2-rule ../data/phsim/0408/rte1_2

