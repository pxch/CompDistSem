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

/*
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
*/

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DistVec {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Pattern WORD = Pattern.compile("\\w+");

    private static final List<String> VALID_POS = Arrays.asList(
        new String[]{"nn", "vb", "jj", "rb", "in"});

    private static final Map<String, Boolean> VALID_DEP = new HashMap<String, Boolean>() {
        {
            put("amod", true);
            put("nsubj", true);
            put("dobj", false);
            put("pobj", false);
            put("acomp", false);
        };
    };

    private static String checkWord(String w) {
        int slash_idx = w.lastIndexOf("/");
        String pos = w.substring(slash_idx + 1, w.length());
        if (pos.length() >= 2) {
            pos = pos.substring(0, 2);
            if (VALID_POS.contains(pos)) {
                String lemma = w.substring(0, slash_idx);
                if (lemma.matches("[a-zA-Z]+")) {
                    return lemma + "-" + pos;
                }
            }
        }
        return "\t" + w;
    }

    private static List<String> getWordList(String s) {
        String[] rawWords = s.split("\t#DEP#\t")[0].toLowerCase().split(" ");
        List<String> words = new ArrayList<String>();
        for (String w : rawWords) {
            words.add(checkWord(w));
        }
        return words;
    }

    private static List<String> getDepList(String s, String label) {
        List<String> deps = new ArrayList<String>();

        if (s.split("\t#DEP#\t").length < 2) {
            return deps;
        }

        String[] rawDeps = s.split("\t#DEP#\t")[1].split(" ");
        for (String d : rawDeps) {
            if (label.equals(d.substring(0, d.indexOf("-")))) {
                deps.add(d);
            }
        }
        return deps;
    }

    public static class ReadCounts implements PairFunction<String, String, Integer> {
        @Override
        public Tuple2<String, Integer> call(String s) {
            String ss = s.substring(1, s.length() - 1);
            String word = ss.split(",")[0];
            int count = Integer.valueOf(ss.split(",")[1]);
            return new Tuple2<String, Integer>(word, count);
        }
    }

    public static class ParseWord implements PairFlatMapFunction<String, String, Integer> {
        @Override
        public Iterable<Tuple2<String, Integer>> call(String s) {
            List<Tuple2<String, Integer>> terms = new ArrayList<Tuple2<String, Integer>>();

            List<String> words = getWordList(s);

            for (String w : words) {
                if (!w.startsWith("\t")) {
                    terms.add(new Tuple2<String, Integer>(w, 1));
                }
            }

            return terms;
        }
    }

    public static class ParsePhrase implements PairFlatMapFunction<String, String, Integer> {
        String label;
        public ParsePhrase(String l) {
            label = l;
        }

        @Override
        public Iterable<Tuple2<String, Integer>> call(String s) {
            List<Tuple2<String, Integer>> terms = new ArrayList<Tuple2<String, Integer>>();

            List<String> words = getWordList(s);
            List<String> deps = getDepList(s, label);

//            String label;
            int gov_idx, dep_idx, pos1, pos2;
            String w1, w2;
            for (String d : deps) {
                pos2 = d.lastIndexOf("-");
                pos1 = d.lastIndexOf("-", pos2 - 1);
//                label = d.substring(0, pos1);
                gov_idx = Integer.valueOf(d.substring(pos1 + 1, pos2));
                dep_idx = Integer.valueOf(d.substring(pos2 + 1, d.length()));

                if (VALID_DEP.get(label) == true) {
                    w1 = words.get(dep_idx);
                    w2 = words.get(gov_idx);
                } else {
                    w1 = words.get(gov_idx);
                    w2 = words.get(dep_idx);
                }

                if (!w1.startsWith("\t") && !w2.startsWith("\t")) {
                    terms.add(new Tuple2<String, Integer>(w1 + "+" + w2, 1));
                    /*
                    if ("amod".equals(label)) {
                        if (w1.endsWith("jj") && w2.endsWith("nn")) {
                            terms.add(new Tuple2<String, Integer>(w1 + "+" + w2, 1));
                        }
                    }
                    else if ("nsubj".equals(label)) {
                        if (w1.endsWith("nn") && w2.endsWith("vb")) {
                            terms.add(new Tuple2<String, Integer>(w1 + "+" + w2, 1));
                        }
                    }
                    else if ("dobj".equals(label)) {
                        if (w1.endsWith("vb") && w2.endsWith("nn")) {
                            terms.add(new Tuple2<String, Integer>(w1 + "+" + w2, 1));
                        }
                    }
                    else if ("pobj".equals(label)) {
                        if (w1.endsWith("in") && w2.endsWith("nn")) {
                            terms.add(new Tuple2<String, Integer>(w1 + "+" + w2, 1));
                        }
                    }
                    */
                }
            }
            return terms;
        }
    }

    public static class ParseWordPair implements PairFlatMapFunction<String, String, Integer> {
        int windowSize;

        public ParseWordPair(int ws) {
            windowSize = ws;
        }

        @Override
        public Iterable<Tuple2<String, Integer>> call(String s) {
            List<String> rawWords = getWordList(s);
            List<String> words = new ArrayList<String>();
            for (String w : rawWords) {
                if (!w.startsWith("\t")) {
                    words.add(w);
                }
            }

            List<Tuple2<String, Integer>> terms = new ArrayList<Tuple2<String, Integer>>();
            String w1, w2;
            for (int ws = 1; ws <= windowSize; ++ws) {
                for (int i = 0; i < words.size() - ws; ++i) {
                    w1 = words.get(i);
                    w2 = words.get(i+ws);
                    String pair = w1.compareTo(w2) < 0 ? w1 + "\t" + w2 : w2 + "\t" + w1;
                    terms.add(new Tuple2<String, Integer>(pair, 1));
                }
            }

            return terms;
        }
    }

    public static class ParsePhrasePair implements PairFlatMapFunction<String, String, Integer> {
        int windowSize;
        String label;

        public ParsePhrasePair(int ws, String l) {
            windowSize = ws;
            label = l;
        }

        @Override
        public Iterable<Tuple2<String, Integer>> call(String s) {
            List<String> words = getWordList(s);
            List<String> deps = getDepList(s, label);

//            String label;
            int gov_idx, dep_idx, pos1, pos2, idx1, idx2;
            String w1, w2;
            String phrase;
            int cnt, w_idx, sep_idx;
            String word;

            List<Tuple2<String, Integer>> terms = new ArrayList<Tuple2<String, Integer>>();

            for (String d : deps) {
                pos2 = d.lastIndexOf("-");
                pos1 = d.lastIndexOf("-", pos2 - 1);
//                label = d.substring(0, pos1);
                gov_idx = Integer.valueOf(d.substring(pos1 + 1, pos2));
                dep_idx = Integer.valueOf(d.substring(pos2 + 1, d.length()));

                if (VALID_DEP.get(label) == true) {
                    w1 = words.get(dep_idx);
                    w2 = words.get(gov_idx);
                } else {
                    w1 = words.get(gov_idx);
                    w2 = words.get(dep_idx);
                }

                if (w1.startsWith("\t") || w2.startsWith("\t")) {
                    continue;
                }
                phrase = w1 + "+" + w2;

                if (gov_idx > dep_idx) {
                    idx1 = dep_idx;
                    idx2 = gov_idx;
                } else {
                    idx1 = gov_idx;
                    idx2 = dep_idx;
                }

                // Add words on the left hand side of the phrase
                cnt = 0;
                w_idx = idx1 - 1;
                while (cnt < windowSize && w_idx >= 0) {
                    word = words.get(w_idx);
                    if (!word.startsWith("\t")) {
                        terms.add(new Tuple2<String, Integer>(phrase + "\t" + word, 1));
                        ++cnt;
                    }
                    --w_idx;
                }

                // Add words on the right hand side of the phrase
                cnt = 0;
                w_idx = idx2 + 1;
                while (cnt < windowSize && w_idx < words.size()) {
                    word = words.get(w_idx);
                    if (!word.startsWith("\t")) {
                        terms.add(new Tuple2<String, Integer>(phrase + "\t" + word, 1));
                        ++cnt;
                    }
                    ++ w_idx;
                }

                // Add words within the phrase
                cnt = 0;
                w_idx = idx1 + 1;
                while (cnt < windowSize && w_idx < idx2) {
                    word = words.get(w_idx);
                    if (!word.startsWith("\t")) {
                        terms.add(new Tuple2<String, Integer>(phrase + "\t" + word, 1));
                        ++cnt;
                    }
                    ++ w_idx;
                }
                if (idx2 - idx1 > 2) {
                    cnt = 0;
                    sep_idx = w_idx;
                    w_idx = idx2 - 1;
                    while (cnt < windowSize && w_idx >= sep_idx) {
                        word = words.get(w_idx);
                        if (!word.startsWith("\t")) {
                            terms.add(new Tuple2<String, Integer>(phrase + "\t" + word, 1));
                            ++cnt;
                        }
                        --w_idx;
                    }
                }
            }

            return terms;
        }
    }

    public static class ComputeWordPMI implements
        FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Double>> {

        Map<String, Tuple2<Integer, Integer>> wordCountIdxMap;
        long wordTotalCount, wordPairTotalCount;
        int dim;

        public ComputeWordPMI(long wtc, long wptc, Map<String, Tuple2<Integer, Integer>> map, int d) {
            wordCountIdxMap = map;
            wordTotalCount = wtc;
            wordPairTotalCount = wptc;
            dim = d;
        }

        @Override
        public Iterable<Tuple2<String, Double>> call(Tuple2<String, Integer> t) {
            String pair = t._1;
            int pairCount = t._2;
            String word1 = pair.split("\t")[0];
            String word2 = pair.split("\t")[1];
            int word1Count, word2Count, word1Idx, word2Idx;
            Tuple2<Integer, Integer> countIdxPair;

            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
            if (!wordCountIdxMap.containsKey(word1) || !wordCountIdxMap.containsKey(word2)) {
                return results;
            }

            countIdxPair = wordCountIdxMap.get(word1);
            word1Count = countIdxPair._1;
            word1Idx = countIdxPair._2;
            countIdxPair = wordCountIdxMap.get(word2);
            word2Count = countIdxPair._1;
            word2Idx = countIdxPair._2;
            
            double pairProb = (double) pairCount / wordPairTotalCount;
            double word1Prob = (double) word1Count / wordTotalCount;
            double word2Prob = (double) word2Count / wordTotalCount;
            double pmi = Math.log(pairProb / (word1Prob * word2Prob));

            if (pmi > 0) {
/*
                String pairIdx = word1Idx < word2Idx ?
                    Integer.toString(word1Idx) + "\t" + Integer.toString(word2Idx) :
                    Integer.toString(word2Idx) + "\t" + Integer.toString(word1Idx);
                results.add(new Tuple2<String, Double>(pairIdx, pmi));
*/
                if (word2Idx < dim) {
                    String pairIdx =
                        Integer.toString(word1Idx) + "\t" + Integer.toString(word2Idx);
                    results.add(new Tuple2<String, Double>(pairIdx, pmi));
                }
                if (word1Idx < dim) {
                    String pairIdx =
                        Integer.toString(word2Idx) + "\t" + Integer.toString(word1Idx);
                    results.add(new Tuple2<String, Double>(pairIdx, pmi));
                }
            }
            return results;
        }
    }

    public static class ComputePhrasePMI implements
        FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Double>> {

        Map<String, Tuple2<Integer, Integer>> wordCountIdxMap, phraseCountIdxMap;
        long wordTotalCount, phraseTotalCount, phrasePairTotalCount;
        int dim;

        public ComputePhrasePMI(long wtc, Map<String, Tuple2<Integer, Integer>> wmap,
            long ptc, long pptc, Map<String, Tuple2<Integer, Integer>> pmap, int d) {
            wordCountIdxMap = wmap;
            wordTotalCount = wtc;
            phraseCountIdxMap = pmap;
            phraseTotalCount = ptc;
            phrasePairTotalCount = pptc;
            dim = d;
        }

        @Override
        public Iterable<Tuple2<String, Double>> call(Tuple2<String, Integer> t) {
            String pair = t._1;
            int pairCount = t._2;
            String phrase = pair.split("\t")[0];
            String word = pair.split("\t")[1];
            int phraseCount, wordCount, phraseIdx, wordIdx;
            Tuple2<Integer, Integer> countIdxPair;

            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
            if (!phraseCountIdxMap.containsKey(phrase) || !wordCountIdxMap.containsKey(word)) {
                return results;
            }

            countIdxPair = phraseCountIdxMap.get(phrase);
            phraseCount = countIdxPair._1;
            phraseIdx = countIdxPair._2;
            countIdxPair = wordCountIdxMap.get(word);
            wordCount = countIdxPair._1;
            wordIdx = countIdxPair._2;
            
            double pairProb = (double) pairCount / phrasePairTotalCount;
            double phraseProb = (double) phraseCount / phraseTotalCount;
            double wordProb = (double) wordCount / wordTotalCount;
            double pmi = Math.log(pairProb / (phraseProb * wordProb));

            if (pmi > 0 && wordIdx < dim) {
                String pairIdx = Integer.toString(phraseIdx) + "\t" + Integer.toString(wordIdx);
                results.add(new Tuple2<String, Double>(pairIdx, pmi));
            }
            return results;
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

    public static class PhraseFilter implements Function<Tuple2<String, Integer>, Boolean> {
        List<String> wordVocabList;
        public PhraseFilter(List<String> vocab) {
            wordVocabList = vocab;
        }

        @Override
        public Boolean call(Tuple2<String, Integer> t) {
            String w1 = t._1.split("\\+")[0];
            String w2 = t._1.split("\\+")[1];
            return wordVocabList.contains(w1) && wordVocabList.contains(w2);
        }
    }

    public static class Sum implements Function2<Integer, Integer, Integer> {
        @Override
        public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
        }
    }

    public static class WordCountComparator implements
        Comparator<Tuple2<String, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            if (t1._2 < t2._2) {
                return -1;
            } else if (t1._2 > t2._2) {
                return 1;
            } else {
                return t1._1.compareTo(t2._1);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        String usage_str = "Usage: DistVec inputFile outputPath windowSize numPartitions" +
            " word|amod|nsubj|dobj|pobj|acomp [--loadWordCount] [--vocabSize v] [--dim d]";
        if (args.length < 5) {
            System.err.println(usage_str);
            System.exit(1);
        }

        String file = args[0];
        String path = args[1];
        int windowSize = Integer.valueOf(args[2]);
        int numPartitions = Integer.valueOf(args[3]);

        String label = args[4];
        if (!"word".equals(label) && !"amod".equals(label) && !"nsubj".equals(label) &&
            !"dobj".equals(label) && !"pobj".equals(label) && !"acomp".equals(label)) {
            System.err.println(usage_str);
            System.exit(1);
        }

        boolean loadWordCount = false;
        int vocabSize = 50000;
        int dim = 50000;
/*
        if (args.length > 5) {
            if ("--loadWordCount".equals(args[5])) {
                loadWordCount = true;
            } else {
                System.err.println(usage_str);
                System.exit(1);
            }
        }
*/
        if (args.length > 5) {
            int arg_idx = 5;
            while (arg_idx < args.length) {
                if ("--loadWordCount".equals(args[arg_idx])) {
                    loadWordCount = true;
                    arg_idx ++;
                } else if ("--vocabSize".equals(args[arg_idx])) {
                    vocabSize = Integer.valueOf(args[arg_idx+1]);
                    arg_idx += 2;
                } else if ("--dim".equals(args[arg_idx])) {
                    dim = Integer.valueOf(args[arg_idx+1]);
                    arg_idx += 2;
                } else {
                    System.err.println(usage_str);
                    System.exit(1);
                }
            }
        }

        if (dim > vocabSize) {
            System.out.format("Warning! Dimension %d > Vocabulary size %d!", dim, vocabSize);
            System.out.println(" Setting dimension to vocabulary size"); 
            dim = vocabSize;
        }

//        System.out.format("Dimension %d, Vocabulary size %d, Label " + label + ", loadWord %b.%n", dim, vocabSize, loadWordCount);

//        System.exit(0);

        SparkConf sparkConf = new SparkConf().setAppName("Distributional Vector")
            .set("spark.executor.memory", "4g").set("spark.driver.memory", "4g");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = ctx.textFile(file, numPartitions);

        JavaPairRDD<String, Integer> wordCounts;
        JavaRDD<String> wordVocab;

        long wordTotalCount = 0;

        if (!loadWordCount) {
            // Parse words
            JavaPairRDD<String, Integer> wordOnes = lines.flatMapToPair(new ParseWord());
            wordTotalCount = wordOnes.count();

            BufferedWriter writer = new BufferedWriter(new FileWriter("./word_total_count"));
            writer.write(String.valueOf(wordTotalCount) + "\n");
            writer.close();

//            wordCounts = wordOnes.reduceByKey(new Sum()).filter(new CountFilter(20));
            wordCounts = wordOnes.reduceByKey(new Sum());
/*
 * Sort word counts
            wordCounts = wordCounts
                .mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> t) {
                        return new Tuple2<Integer, String>(t._2, t._1);
                    }})
                .sortByKey(false)
                .mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> t) {
                        return new Tuple2<String, Integer>(t._2, t._1);
                    }});
*/
            int topCnt;
            if (wordCounts.count() > vocabSize) {
                topCnt = vocabSize;
            } else {
                topCnt = (int) wordCounts.count();
            }
            wordCounts = ctx.parallelizePairs(
                    wordCounts.top(topCnt, new WordCountComparator()));

            // Write word counts to file
            wordCounts.coalesce(1).saveAsTextFile(path + "/wordcounts");

            // Sort keys from word counts PairRDD to word vocab RDD
//            wordVocab = wordCounts.sortByKey().keys();
            wordVocab = wordCounts.keys();
            wordVocab.coalesce(1).saveAsTextFile(path + "/wordvocab");
        } else {
            BufferedReader reader = new BufferedReader(new FileReader("./word_total_count"));
            wordTotalCount = Integer.valueOf(reader.readLine());
            reader.close();

            wordCounts = ctx.textFile(path + "/wordcounts", 1)
                .mapToPair(new ReadCounts());
            int topCnt;
            if (wordCounts.count() > vocabSize) {
                topCnt = vocabSize;
            } else {
                topCnt = (int) wordCounts.count();
            }
            wordCounts = ctx.parallelizePairs(
                    wordCounts.top(topCnt, new WordCountComparator()));

//            wordVocab = wordCounts.sortByKey().keys();
            wordVocab = wordCounts.keys();
        }

        // Convert word counts PairRDD to HashMap
        Map<String, Integer> wordCountsMap = new HashMap<String, Integer>(
            wordCounts.collectAsMap());

        // Convert word vocab RDD to ArrayList
        List<String> wordVocabList = new ArrayList<String>(wordVocab.collect());

        // Generate map from word to index and count
        Map<String, Tuple2<Integer, Integer>> wordCountIdxMap =
            new HashMap<String, Tuple2<Integer, Integer>>();

        for (int idx = 0; idx < wordVocabList.size(); ++idx) {
            String word = wordVocabList.get(idx);
            int count = wordCountsMap.get(word);
            wordCountIdxMap.put(word, new Tuple2<Integer, Integer>(count, idx));
        }

        wordCountsMap.clear();

        if ("word".equals(label)) {

            // Parse word pairs
            JavaPairRDD<String, Integer> wordPairOnes = lines
                .flatMapToPair(new ParseWordPair(windowSize));
            long wordPairTotalCount = wordPairOnes.count();

            JavaPairRDD<String, Integer> wordPairCounts = wordPairOnes.reduceByKey(new Sum());

            // Compute PMI for word pairs
            JavaPairRDD<String, Double> wordPairPMI = JavaPairRDD.fromJavaRDD(wordPairCounts
                .flatMap(
                    new ComputeWordPMI(wordTotalCount, wordPairTotalCount, wordCountIdxMap, dim)));

            wordPairPMI.coalesce(10).saveAsTextFile(path + "/wordpmi");

        } else {

            // Parse phrases
            JavaPairRDD<String, Integer> phraseOnes = lines
                .flatMapToPair(new ParsePhrase(label));
            long phraseTotalCount = phraseOnes.count();

            JavaPairRDD<String, Integer> phraseCounts = phraseOnes.reduceByKey(new Sum())
                .filter(new CountFilter(20))
                .filter(new PhraseFilter(wordVocabList));

            // Write phrase counts to file
            phraseCounts.coalesce(20).saveAsTextFile(path + "/" + label + "-phrasecounts");

            // Convert phrase counts PairRDD to HashMap
            Map<String, Integer> phraseCountsMap = new HashMap<String, Integer>(
                phraseCounts.collectAsMap());

            // Sort keys from phrase counts PairRDD to phrase vocab RDD
            JavaRDD<String> phraseVocab = phraseCounts.sortByKey().keys();
            phraseVocab.coalesce(20).saveAsTextFile(path + "/" + label + "-phrasevocab");

            // Convert phrase vocab RDD to ArrayList
            List<String> phraseVocabList = new ArrayList<String>(phraseVocab.collect());

            // Generate map from word to index and count
            Map<String, Tuple2<Integer, Integer>> phraseCountIdxMap =
                new HashMap<String, Tuple2<Integer, Integer>>();

            for (int idx = 0; idx < phraseVocabList.size(); ++idx) {
                String phrase = phraseVocabList.get(idx);
                int count = phraseCountsMap.get(phrase);
                phraseCountIdxMap.put(phrase, new Tuple2<Integer, Integer>(count, idx));
            }

            phraseCountsMap.clear();
            phraseVocabList.clear();

            // Parse phrase pairs
            JavaPairRDD<String, Integer> phrasePairOnes = lines
                .flatMapToPair(new ParsePhrasePair(windowSize, label));
            long phrasePairTotalCount = phrasePairOnes.count();

            JavaPairRDD<String, Integer> phrasePairCounts = phrasePairOnes
                .reduceByKey(new Sum());

            // Compute PMI for word pairs
            JavaPairRDD<String, Double> phrasePairPMI = JavaPairRDD.fromJavaRDD(phrasePairCounts
                .flatMap(
                    new ComputePhrasePMI(wordTotalCount, wordCountIdxMap,
                        phraseTotalCount, phrasePairTotalCount, phraseCountIdxMap, dim)));

            phrasePairPMI.coalesce(50).saveAsTextFile(path + "/" + label + "-phrasepmi");
        }
/*
*/
    }
}

