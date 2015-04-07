#!/bin/sh

SPARK_CMD="spark-submit --master yarn-client --driver-memory 4G --executor-memory 8G --executor-cores 1 --num-executors 50"

CLASS_NAME="edu.utexas.deft.DistVec target/dist-vec-1.0.jar"

INPUT=$1

OUTPUT=$2

OPT="--vocabSize 50000 --dim 20000"

#$SPARK_CMD --class $CLASS_NAME $INPUT $OUTPUT 2 100 word $OPT
#$SPARK_CMD --class $CLASS_NAME $INPUT $OUTPUT 2 100 amod --loadWordCount $OPT
#$SPARK_CMD --class $CLASS_NAME $INPUT $OUTPUT 2 100 nsubj --loadWordCount $OPT
#$SPARK_CMD --class $CLASS_NAME $INPUT $OUTPUT 2 100 dobj --loadWordCount $OPT
#$SPARK_CMD --class $CLASS_NAME $INPUT $OUTPUT 2 100 pobj --loadWordCount $OPT
$SPARK_CMD --class $CLASS_NAME $INPUT $OUTPUT 2 100 acomp --loadWordCount $OPT

