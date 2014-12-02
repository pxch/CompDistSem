package edu.utexas.deft;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DistVec {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Pattern WORD = Pattern.compile("\\w+");

    public static class ParseWord implements PairFlatMapFunction<String, String, Integer> {
        @Override
        public Iterable<Tuple2<String, Integer>> call(String s) {
            Matcher m = WORD.matcher(s.toLowerCase());
            List<String> words = Arrays.asList(SPACE.split(s.toLowerCase()));
            List<Tuple2<String, Integer>> terms = new ArrayList<Tuple2<String, Integer>>();
            for (int i = 0; i < words.size(); ++i) {
                terms.add(new Tuple2<String, Integer>(words.get(i), 1));
            }
            return terms;
        }
    }

    public static class ParsePair implements PairFlatMapFunction<String, String, Integer> {
        int windowSize;
        Map<String, Integer> wordCountsMap;

        public ParsePair(int ws, Map<String, Integer> map) {
            windowSize = ws;
            wordCountsMap = map;
        }

        @Override
        public Iterable<Tuple2<String, Integer>> call(String s) {
            Matcher m = WORD.matcher(s.toLowerCase());

            List<String> rawWords = Arrays.asList(SPACE.split(s.toLowerCase()));

            List<String> words = new ArrayList<String>();
            for (int i = 0; i < rawWords.size(); ++i) {
                if (wordCountsMap.containsKey(rawWords.get(i))) {
                    words.add(rawWords.get(i));
                }
            }

            List<String> pairs = new ArrayList<String>();
            String word1, word2;
            for (int w = 1; w <= windowSize; ++w) {
                for (int i = 0; i < words.size() - w; ++i) {
                    word1 = words.get(i);
                    word2 = words.get(i+w);
//                    if (wordCountsMap.containsKey(word1) && wordCountsMap.containsKey(word2)) {
                        String pair = word1.compareTo(word2) < 0 ?
                            word1 + "\t" + word2 : word2 + "\t" + word1;
                        pairs.add(pair);
//                    }
                }
            }

            List<Tuple2<String, Integer>> terms = new ArrayList<Tuple2<String, Integer>>();
            for (int i = 0; i < pairs.size(); ++i) {
                terms.add(new Tuple2<String, Integer>(pairs.get(i), 1));
            }

            return terms;
        }
    }

    public static class CountFilter implements Function<Tuple2<String, Integer>, Boolean> {
        int thres;
        public CountFilter(int t) {
            thres = t;
        }
        @Override
        public Boolean call(Tuple2<String, Integer> t) {
            return (t._2 >= thres);
        }
    }

    public static class Sum implements Function2<Integer, Integer, Integer> {
        @Override
        public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
        }
    }
/*
    public static class WordFilter implements Function<Tuple2<String, Integer>, Boolean> {
        @Override
        public Boolean call(Tuple2<String, Integer> t) {
            return !t._1.contains("\t");
        }
    }

    public static class PairFilter implements Function<Tuple2<String, Integer>, Boolean> {
        @Override
        public Boolean call(Tuple2<String, Integer> t) {
            return t._1.contains("\t");
        }
    }
*/
    public static class ComputePMI implements
        Function<Tuple2<String, Integer>, Tuple2<String, Double>> {

        Map<String, Tuple2<Integer, Integer>> wordCountIdxMap;
        long wordTotalCount;
        long pairTotalCount;

        public ComputePMI(long wc, long pc, Map<String, Tuple2<Integer, Integer>> map) {
            wordCountIdxMap = map;
            wordTotalCount = wc;
            pairTotalCount = pc;
        }

        @Override
        public Tuple2<String, Double> call(Tuple2<String, Integer> t) {
            String pair = t._1;
            int pairCount = t._2;
            String word1 = pair.split("\t")[0];
            String word2 = pair.split("\t")[1];
            int word1Count, word2Count, word1Idx, word2Idx;
            Tuple2<Integer, Integer> countIdxPair;
            if (wordCountIdxMap.containsKey(word1)) {
                countIdxPair = wordCountIdxMap.get(word1);
                word1Count = countIdxPair._1;
                word1Idx = countIdxPair._2;
            } else {
                word1Count = 0;
                word1Idx = -1;
            }
            if (wordCountIdxMap.containsKey(word2)) {
                countIdxPair = wordCountIdxMap.get(word2);
                word2Count = countIdxPair._1;
                word2Idx = countIdxPair._2;
            } else {
                word2Count = 0;
                word2Idx = -1;
            }
            
            double pmi;
            if (word1Count == 0 || word2Count == 0) {
                pmi = 0.0;
            } else {
                double pairProb = (double) pairCount / pairTotalCount;
                double word1Prob = (double) word1Count / wordTotalCount;
                double word2Prob = (double) word2Count / wordTotalCount;
                pmi = Math.log(pairProb / (word1Prob * word2Prob));
            }
            String pairIdx = word1Idx < word2Idx ?
                Integer.toString(word1Idx) + "\t" + Integer.toString(word2Idx) :
                Integer.toString(word2Idx) + "\t" + Integer.toString(word1Idx);
            return new Tuple2<String, Double>(pairIdx, pmi);
        }
    }

    public static class SparseVecMapper implements
        PairFlatMapFunction<Tuple2<String, Double>, String, String> {
        
        static DecimalFormat formatter = new DecimalFormat("#.######");

        @Override
        public Iterable<Tuple2<String, String>> call(Tuple2<String, Double> pmi) {
            List<Tuple2<String, String>> results = new ArrayList<Tuple2<String, String>>();
            String pair = pmi._1;
            String value = formatter.format(pmi._2);
            String[] words = pair.split("\t");
            results.add(new Tuple2<String, String>(words[0], words[1] + ":" + value));
            results.add(new Tuple2<String, String>(words[1], words[0] + ":" + value));
            return results;
        }
    }

    public static class SparseVecReducer implements
        Function2<String, String, String> {
        @Override
        public String call(String left, String right) {
            return left + "\t" + right;
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: DistVec <inputFile> <outputPath> <windowSize>");
            System.exit(1);
        }

        String file = args[0];
        String path = args[1];
        int windowSize = Integer.valueOf(args[2]);

        SparkConf sparkConf = new SparkConf().setAppName("Distributional Vector");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = ctx.textFile(file, 1);

        JavaPairRDD<String, Integer> wordOnes = lines.flatMapToPair(new ParseWord());
        long wordTotalCount = wordOnes.count();

        JavaPairRDD<String, Integer> wordCounts = wordOnes.reduceByKey(new Sum())
            .filter(new CountFilter(10));

        wordCounts.saveAsTextFile(path + "/wordcounts");

        Map<String, Integer> wordCountMap = new HashMap<String, Integer>(
            wordCounts.collectAsMap());

        JavaPairRDD<String, Integer> pairOnes = lines.flatMapToPair(
            new ParsePair(windowSize, wordCountMap));
        long pairTotalCount = pairOnes.count();

        JavaPairRDD<String, Integer> pairCounts = pairOnes.reduceByKey(new Sum());

        JavaRDD<String> wordList = wordCounts.sortByKey().keys();
        wordList.saveAsTextFile(path + "/vocabulary");

        ArrayList<String> wordArrayList = new ArrayList<String>(wordList.collect());

        Map<String, Tuple2<Integer, Integer>> wordCountIdxMap =
            new HashMap<String, Tuple2<Integer, Integer>>();

        for (int idx = 0; idx < wordArrayList.size(); ++idx) {
            String word = wordArrayList.get(idx);
            int count = wordCountMap.get(word);
            wordCountIdxMap.put(word, new Tuple2<Integer, Integer>(count, idx));
        }

        JavaPairRDD<String, Double> pairPMI = JavaPairRDD.fromJavaRDD(
            pairCounts.map(
                new ComputePMI(wordTotalCount, pairTotalCount, wordCountIdxMap)));

        pairPMI.saveAsTextFile(path + "/pmi");

        JavaPairRDD<String, String> sparseVec = pairPMI
            .flatMapToPair(new SparseVecMapper())
            .reduceByKey(new SparseVecReducer());

        sparseVec.saveAsTextFile(path + "/sparsevec");

    }
}

