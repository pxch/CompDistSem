#!/bin/bash
#----------------------------------------------------
# Example SLURM job script to run OpenMP applications
# on TACC's Stampede system.
#----------------------------------------------------
#SBATCH -J distvec_train_modifier_matrices       # Job name
#SBATCH -o distvec_train_modifier_matrices.o%j   # Name of stdout output file(%j expands to jobId)
#SBATCH -e distvec_train_modifier_matrices.o%j   # Name of stderr output file(%j expands to jobId)
#SBATCH -p gpu              # Submit to the 'normal' or 'development' queue
#SBATCH -N 1                # Total number of nodes requested (16 cores/node)
#SBATCH -n 1                # Total number of mpi tasks requested
#SBATCH -t 08:00:00         # Run time (hh:mm:ss) - 1.5 hours
# The next line is required if the user has more than one project
# #SBATCH -A UT-DEFT  # Allocation name to charge job against

# This example will run an OpenMP application using 16 threads

# Set the number of threads per task(Default=1)
export OMP_NUM_THREADS=16

make trainplf
# Run the OpenMP application
#./trainplf ../corpora/ukwac_1_50k_20k_2 ../corpora/word_count.txt mod_mat_50k_20k_300_1/ 300
#./trainplf ../corpora/ukwac_2 ../corpora/word_count.txt mod_mat_3/ 100
./trainplf ../../corpora/bnc+ukwac+wiki ../data/wordlist/sick+rte- ../data/mod_mat_bnc+ukwac+wiki/ 300

