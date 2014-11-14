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

import java.io.Serializable;
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

    public static class ParseLine implements PairFlatMapFunction<String, String, Integer> {
        int windowSize;

        public ParseLine(int ws) {
            windowSize = ws;
        }

        @Override
        public Iterable<Tuple2<String, Integer>> call(String s) {
            Matcher m = WORD.matcher(s.toLowerCase());
/*
            List<String> words = new ArrayList<String>();
            while (m.find()) {
                words.add(m.group());
            }
*/
            List<String> words = Arrays.asList(SPACE.split(s.toLowerCase()));

            List<String> pairs = new ArrayList<String>();
            for (int w = 1; w <= windowSize; ++w) {
                for (int i = 0; i < words.size() - w; ++i) {
                    String pair = words.get(i).compareTo(words.get(i+w)) < 0 ?
                        words.get(i) + "\t" + words.get(i+w) :
                        words.get(i+w) + "\t" + words.get(i);
                    pairs.add(pair);
                }
            }

            List<Tuple2<String, Integer>> terms = new ArrayList<Tuple2<String, Integer>>();
            for (int i = 0; i < words.size(); ++i) {
                terms.add(new Tuple2<String, Integer>(words.get(i), 1));
            }
            for (int i = 0; i < pairs.size(); ++i) {
                terms.add(new Tuple2<String, Integer>(pairs.get(i), 1));
            }

            return terms;
        }
    }

    public static class Sum implements Function2<Integer, Integer, Integer> {
        @Override
        public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
        }
    }

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

    public static class ComputePMI implements
        Function<Tuple2<String, Integer>, Tuple2<String, Double>>, Serializable {

        Map<String, Integer> wordCountMap;
        List<String> wordList;
        long wordTotalCount;
        long pairTotalCount;

        public ComputePMI(long wc, long pc, Map<String, Integer> wm, List<String> wl) {
            wordCountMap = wm;
            wordList = wl;
            wordTotalCount = wc;
            pairTotalCount = pc;
        }

        @Override
        public Tuple2<String, Double> call(Tuple2<String, Integer> t) {
            String pair = t._1;
            int pairCount = t._2;
            String word1 = pair.split("\t")[0];
            String word2 = pair.split("\t")[1];
            int word1Count = wordCountMap.containsKey(word1) ? wordCountMap.get(word1) : 0;
            int word2Count = wordCountMap.containsKey(word2) ? wordCountMap.get(word2) : 0;
            double pmi;
            if (word1Count == 0 || word2Count == 0) {
                pmi = 0.0;
            } else {
                double pairProb = (double) pairCount / pairTotalCount;
                double word1Prob = (double) word1Count / wordTotalCount;
                double word2Prob = (double) word2Count / wordTotalCount;
                pmi = Math.log(pairProb / (word1Prob * word2Prob));
            }
            int word1Idx = wordList.indexOf(word1);
            int word2Idx = wordList.indexOf(word2);
            String pairIdx = word1Idx < word2Idx ?
                Integer.toString(word1Idx) + "\t" + Integer.toString(word2Idx) :
                Integer.toString(word2Idx) + "\t" + Integer.toString(word1Idx);
            return new Tuple2<String, Double>(pairIdx, pmi);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: DistVec <inputFile> <outputPath> <windowSize>");
            System.exit(1);
        }

        String file = args[0];
        String path = args[1];
        int ws = Integer.valueOf(args[2]);

        SparkConf sparkConf = new SparkConf().setAppName("Distributional Vector");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = ctx.textFile(file, 1);

        JavaPairRDD<String, Integer> ones = lines.flatMapToPair(new ParseLine(ws));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Sum());

        long wordTotalCount = ones.filter(new WordFilter()).count();
        long pairTotalCount = ones.filter(new PairFilter()).count();

        JavaPairRDD<String, Integer> wordCounts = counts.filter(new WordFilter());
//        wordCounts.persist(StorageLevel.MEMORY_ONLY_SER());

//        Map<String, Integer> wordCountsMap = wordCounts.collectAsMap();

//        Map<String, Integer> tmpMap = wordCounts.collectAsMap();
//        Map<String, Integer> wordCountsMap = new HashMap<String, Integer>(tmpMap);
        Map<String, Integer> wordCountMap = new HashMap<String, Integer>(
            wordCounts.collectAsMap());

        ArrayList<String> wordList = new ArrayList<String>(
            wordCounts.sortByKey().keys().collect());

        JavaPairRDD<String, Integer> pairCounts = counts.filter(new PairFilter());

//        wordCounts.saveAsTextFile(path + "/wordCounts");
//        pairCounts.saveAsTextFile(path + "/pairCounts");

//        pairCounts.persist(StorageLevel.MEMORY_ONLY_SER());
/*
        ComputePMI computePMI = new ComputePMI(wordTotalCount, pairTotalCount, wordCountsMap);

        JavaPairRDD<String, Double> pairPMI = JavaPairRDD.fromJavaRDD(
            pairCounts.map(computePMI));
*/
        JavaPairRDD<String, Double> pairPMI = JavaPairRDD.fromJavaRDD(
            pairCounts.map(
                new ComputePMI(wordTotalCount, pairTotalCount, wordCountMap, wordList)));

        pairPMI.saveAsTextFile(path + "/pmi");

/*
        System.out.println("word vocabulary size: " + wordCounts.count());
        System.out.println("pair vocabulary size: " + pairCounts.count());
        System.out.println("total vocabulary size: " + counts.count());

        counts.saveAsTextFile(path + "/counts");
/*
        JavaRDD<String> words = getWords(lines);
        JavaPairRDD<String, Integer> wordCounts = getCounts(words);
        wordCounts.saveAsTextFile(path + "/wordCounts");

        JavaRDD<String> pairs = getPairs(lines, ws);
        JavaPairRDD<String, Integer> pairCounts = getCounts(pairs);
        pairCounts.saveAsTextFile(path + "/pairCounts");
*/
    }
}

