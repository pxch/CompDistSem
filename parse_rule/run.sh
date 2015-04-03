#!/bin/bash
#----------------------------------------------------
# Example SLURM job script to run OpenMP applications
# on TACC's Stampede system.
#----------------------------------------------------
#SBATCH -J parse_phrasal_rules       # Job name
#SBATCH -o parse_phrasal_rules.o%j   # Name of stdout output file(%j expands to jobId)
#SBATCH -e parse_phrasal_rules.o%j   # Name of stderr output file(%j expands to jobId)
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
javac -cp ../lib/stanford-corenlp-3.4.jar ParseRule.java
java -Xmx4g -cp .:../lib/stanford-corenlp-3.4.jar:../lib/stanford-corenlp-3.4-models.jar ParseRule diffRules-30-allSures-filtered output_new_3_train
java -Xmx4g -cp .:../lib/stanford-corenlp-3.4.jar:../lib/stanford-corenlp-3.4-models.jar ParseRule diffRules-30-allSures-test-filtered output_new_3_test

