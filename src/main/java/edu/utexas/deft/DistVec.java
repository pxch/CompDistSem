package edu.utexas.deft;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DistVec {
//    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Pattern WORD = Pattern.compile("\\w+");

    public static JavaRDD<String> getWords(JavaRDD<String> lines) {
        JavaRDD<String> words = lines.flatMap(
            new FlatMapFunction<String, String>() {
                @Override
                public Iterable<String> call(String s) {
                    List<String> words = new ArrayList<String>();
                    Matcher m = WORD.matcher(s.toLowerCase());
                    while (m.find()) {
                        words.add(m.group());
                    }
                    return words;
//                    return Arrays.asList(SPACE.split(s));
                }
            });

        return words;
    }

    public static JavaRDD<String> getPairs(JavaRDD<String> lines, final int ws) {
        JavaRDD<String> pairs = lines.flatMap(
            new FlatMapFunction<String, String>() {
                @Override
                public Iterable<String> call(String s) {
                    List<String> words = new ArrayList<String>();
                    Matcher m = WORD.matcher(s.toLowerCase());
                    while (m.find()) {
                        words.add(m.group());
                    }
                    List<String> pairs = new ArrayList<String>();
                    int start, end;
                    for (int i = 0; i < words.size(); ++i) {
                        start = i - ws > 0 ? i - ws : 0;
                        end = i + ws + 1 < words.size() ? i + ws + 1 : words.size();
                        for (int j = start; j < end; ++j) {
                            if (j != i) {
                                pairs.add(words.get(i) + "\t" + words.get(j));
                            }
                        }
                    }
                    return pairs;
                }
            });

        return pairs;
    }

    public static JavaPairRDD<String, Integer> getCounts(JavaRDD<String> terms) {
        JavaPairRDD<String, Integer> ones = terms.mapToPair(
            new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) {
                    return new Tuple2<String, Integer>(s, 1);
                }
            });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
            new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer i1, Integer i2) {
                    return i1 + i2;
                }
            });

        return counts;
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

        JavaRDD<String> words = getWords(lines);
        JavaPairRDD<String, Integer> wordCounts = getCounts(words);
        wordCounts.saveAsTextFile(path + "/wordCounts");

        JavaRDD<String> pairs = getPairs(lines, ws);
        JavaPairRDD<String, Integer> pairCounts = getCounts(pairs);
        pairCounts.saveAsTextFile(path + "/pairCounts");

    }
}

