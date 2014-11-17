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
/*
            List<String> pairs = new ArrayList<String>();
            for (int w = 1; w <= windowSize; ++w) {
                for (int i = 0; i < words.size() - w; ++i) {
                    String pair = words.get(i).compareTo(words.get(i+w)) < 0 ?
                        words.get(i) + "\t" + words.get(i+w) :
                        words.get(i+w) + "\t" + words.get(i);
                    pairs.add(pair);
                }
            }
*/
            List<Tuple2<String, Integer>> terms = new ArrayList<Tuple2<String, Integer>>();
            for (int i = 0; i < words.size(); ++i) {
                terms.add(new Tuple2<String, Integer>(words.get(i), 1));
            }
/*            for (int i = 0; i < pairs.size(); ++i) {
                terms.add(new Tuple2<String, Integer>(pairs.get(i), 1));
            }
*/
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

        JavaPairRDD<String, Integer> filterCounts = counts.filter(new CountFilter(10));
        filterCounts.saveAsTextFile(path + "/wordCounts");
/*
        long wordTotalCount = ones.filter(new WordFilter()).count();
        long pairTotalCount = ones.filter(new PairFilter()).count();

        JavaPairRDD<String, Integer> wordCounts = counts.filter(new WordFilter());

        Map<String, Integer> wordCountMap = new HashMap<String, Integer>(
            wordCounts.collectAsMap());

        JavaRDD<String> wordList = wordCounts.sortByKey().keys();

        ArrayList<String> wordArrayList = new ArrayList<String>(
            wordCounts.sortByKey().keys().collect());

        Map<String, Tuple2<Integer, Integer>> wordCountIdxMap =
            new HashMap<String, Tuple2<Integer, Integer>>();

        for (int idx = 0; idx < wordArrayList.size(); ++idx) {
            String word = wordArrayList.get(idx);
            int count = wordCountMap.get(word);
            wordCountIdxMap.put(word, new Tuple2<Integer, Integer>(count, idx));
        }

        JavaPairRDD<String, Integer> pairCounts = counts.filter(new PairFilter());

//        wordCounts.saveAsTextFile(path + "/wordCounts");
//        pairCounts.saveAsTextFile(path + "/pairCounts");

        JavaPairRDD<String, Double> pairPMI = JavaPairRDD.fromJavaRDD(
            pairCounts.map(
                new ComputePMI(wordTotalCount, pairTotalCount, wordCountIdxMap)));

        pairPMI.saveAsTextFile(path + "/pmi");

        wordList.saveAsTextFile(path + "/vocabulary");

        BufferedWriter output = new BufferedWriter(new FileWriter("vocabulary.txt"));
        for (int i = 0; i < wordArrayList.size(); ++i) {
            output.write(wordArrayList.get(i) + "\n");
        }
        output.close();

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

