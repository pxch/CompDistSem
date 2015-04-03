import edu.stanford.nlp.io.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.ling.CoreAnnotations.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.semgraph.*;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.*;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.trees.TreeCoreAnnotations.*;
import edu.stanford.nlp.util.*;

import java.io.*;
import java.util.*;

public class ParseRule {
    public static class ParsePhrase {
        public static int compCnt = 0;

        public static List<CoreLabel> tokens;
        public static Set<SemanticGraphEdge> deps;

        public static List<Integer> origPhIs, phIs;

        public static List<String> results;

        public static final List<String> VALID_POS = Arrays.asList(
            new String[]{"nn", "vb", "jj", "rb", "in"});

        public static String getWordStr(CoreLabel token) {
            String lemma = token.get(LemmaAnnotation.class);
            String pos = token.get(PartOfSpeechAnnotation.class).toLowerCase();
            if (pos.length() < 2) {
                return null;
            } else {
                pos = pos.substring(0,2);
                if (!VALID_POS.contains(pos)) {
                    return null;
                }
                return lemma + "-" + pos;
            }
        }

        public static List<Integer> cvtPhStrToIndices(String ph) {
            String[] words = ph.split(" ");
            List<Integer> result = new ArrayList<Integer>();
            for (String w : words) {
                result.add(Integer.valueOf(w.substring(w.lastIndexOf("-") + 1, w.length())));
            }
            return result;
        }

        public static List<Integer> filterIndices(List<Integer> phIs) {
            List<Integer> result = new ArrayList<Integer>();
            for (int idx : phIs) {
                if (getWordStr(tokens.get(idx - 1)) != null) {
                    result.add(idx);
                }
            }
            return result;
        }

        public static String parsePhrase(String ph, Annotation doc) {
            CoreMap sent = doc.get(SentencesAnnotation.class).get(0);
            tokens = sent.get(TokensAnnotation.class);
            deps = sent.get(BasicDependenciesAnnotation.class).getEdgeSet();

            origPhIs = cvtPhStrToIndices(ph);
            phIs = filterIndices(origPhIs);

            results = new ArrayList<String>();
            parsePhraseCore();
            String output = "";
            if (results.size() == 0) {
                output = "#FAIL#";
            } else {
                for (int i = 0; i < results.size(); ++i) {
                    output += String.valueOf(i+1) + " = " + results.get(i) + "\t";
                }
                output = output.trim();
            }
            return output; 
        }

        public static void parsePhraseCore() {
            String result = "";

            if (phIs.size() == 0) {
                return;
            }

            if (phIs.size() == 1) {
                /*
                int idx = phIs.get(0);
                String word = getWordStr(tokens.get(idx-1));
                if (word == null) {
                    //return "#FAIL#";
                    return;
                } else {
                    //result = "#SINGLETON# " + word;
                    result = word;
                    results.add(result);
                }
                //return result;
                */
                result = getWordStr(tokens.get(phIs.get(0) - 1));
                results.add(result);
                return;
            }

            boolean vp = false;
            boolean pp = false;
            boolean np = false;

            for (int idx : phIs) {
                String pos = tokens.get(idx - 1).get(PartOfSpeechAnnotation.class);
                if (pos.startsWith("VB")) {
                    vp = true;
                    //pp = false;
                    //np = false;
                    //rootIdx = idx;
                    //break;
                } else if (pos.startsWith("IN")) {
                    pp = true;
                    //np = false;
                    //rootIdx = idx;
                } else if (pos.startsWith("NN")) {
                    np = true;
                }
            }

            if (!vp & !pp & !np) {
                for (int idx : phIs) {
                    result = getWordStr(tokens.get(idx - 1));
                    //if (result != null) {
                    results.add(result);
                    //}
                }
                return;
            }

/*
            if (vp) {
                pp = false;
                np = false;
                for (int idx : phIs) {
                    String pos = tokens.get(idx - 1).get(PartOfSpeechAnnotation.class);
                    if (pos.startsWith("VB")) {
                        rootIdx = idx;
                        break;
                    }
                }
            }

            if (pp) {
                np = false;
                for (int idx : phIs) {
                    String pos = tokens.get(idx - 1).get(PartOfSpeechAnnotation.class);
                    if (pos.startsWith("IN")) {
                        rootIdx = idx;
                        break;
                    }
                }
            }
 */

            int rootIdx = 0;

            if (vp) {
                for (int idx : phIs) {
                    String pos = tokens.get(idx - 1).get(PartOfSpeechAnnotation.class);
                    if (pos.startsWith("VB")) {
                        rootIdx = idx;
                        break;
                    }
                }

                phIs.remove(phIs.indexOf(rootIdx));
                //result += "#VP# " + getWordStr(tokens.get(rootIdx - 1));
                result += getWordStr(tokens.get(rootIdx - 1));
                int govIdx = 0, depIdx = 0;
                String label;

                compCnt = 1;
                for (SemanticGraphEdge dep : deps) {
                    label = dep.getRelation().toString();
                    govIdx = dep.getGovernor().index();
                    depIdx = dep.getDependent().index();

                    if ("nsubj".equals(label) && govIdx == rootIdx && phIs.contains(depIdx)) {
                        String compLabel = cntLabel(compCnt);
                        compCnt ++;
                        String nounPh = parseNounPhrase(depIdx);
                        //if (nounPh != null) {
                        result += " nsubj-" + compLabel + " ";
                        result += nounPh + " " + compLabel;
                        //}
                    }
                    if ("dobj".equals(label) && govIdx == rootIdx && phIs.contains(depIdx)) {
                        String compLabel = cntLabel(compCnt);
                        compCnt ++;
                        String nounPh = parseNounPhrase(depIdx);
                        //if (nounPh != null) {
                        result += " dobj-" + compLabel + " ";
                        result += nounPh + " " + compLabel;
                        //}
                    }
                    if ("prep".equals(label) && govIdx == rootIdx && phIs.contains(depIdx)) {
                        String compLabel = cntLabel(compCnt);
                        compCnt ++;
                        String prepPh = parsePrepPhrase(depIdx);
                        //if (prepPh != null) {
                        result += " dobj-" + compLabel + " ";
                        result += prepPh + " " + compLabel;
                        //}
                    }
                }
            } else if (pp) {
                for (int idx : phIs) {
                    String pos = tokens.get(idx - 1).get(PartOfSpeechAnnotation.class);
                    if (pos.startsWith("IN")) {
                        rootIdx = idx;
                        break;
                    }
                }

                compCnt = 1;
                String prepPh = parsePrepPhrase(rootIdx);
                //if (prepPh == null) {
                    //return "#FAIL#";
                //}
                //if (prepPh != null) {
                    //result += "#PP# " + prepPh;
                result += prepPh;
                //}
            } else if (np) {
                compCnt = 1;
                for (int idx : phIs) {
                    String word = getWordStr(tokens.get(idx - 1));
                    //if (word != null) {
                    if (word.endsWith("nn")) {
                        //result += "#NP# " + parseNounPhrase(idx) + " ";
                        result += parseNounPhrase(idx);
                        break;
                    }
                    //} else {
                        //phIs.remove(phIs.indexOf(idx));
                    //}
                }
                //result = result.trim();
            }

            //if (result == "") {
                //return "#FAIL#";
            //}

            //return result;
            if (result != "") {
                results.add(result);
            }
            if (!phIs.isEmpty()) {
                parsePhraseCore();
            }
            return;
        }

        public static String cntLabel(int cnt) {
            return "#" + String.valueOf(cnt) + "#";
        }
        
        public static String parsePrepPhrase(int rootIdx) {
            phIs.remove(phIs.indexOf(rootIdx));
            int govIdx = 0, depIdx = 0;
            String label;

            String result = "";
            boolean hasPobj = false;
            String prep = getWordStr(tokens.get(rootIdx - 1));
            /*
            if (prep == null) {
                return null;
            }
            */
            result += getWordStr(tokens.get(rootIdx - 1));
            for (SemanticGraphEdge dep : deps) {
                label = dep.getRelation().toString();
                govIdx = dep.getGovernor().index();
                depIdx = dep.getDependent().index();

                if ("pobj".equals(label) && govIdx == rootIdx) {
                    if (phIs.contains(depIdx)) {
                        hasPobj = true;
                        break;
                    }
                    else {
                        //return null;
                        return result;
                    }
                }
            }
            if (hasPobj == false) {
                //return null;
                return result;
            }
            String compLabel = cntLabel(compCnt);
            compCnt ++;
            String nounPh = parseNounPhrase(depIdx);
            /*
            if (nounPh == null) {
                //return null;
                return result;
            }
            */
            result += " pobj-" + compLabel + " ";
            result += nounPh + " " + compLabel;
            
            return result;
        }
        
        public static String parseNounPhrase(int rootIdx) {
            phIs.remove(phIs.indexOf(rootIdx));
            int govIdx = 0, depIdx = 0;
            String label;

            String result = "", result_tail = "";
            String noun = getWordStr(tokens.get(rootIdx - 1));
            /*
            if (noun == null || !noun.endsWith("nn")) {
                return null;
            }
            */
            for (SemanticGraphEdge dep : deps) {
                label = dep.getRelation().toString();
                govIdx = dep.getGovernor().index();
                depIdx = dep.getDependent().index();

                if ("amod".equals(label) && govIdx == rootIdx) {
                    if (phIs.contains(depIdx)) {
                        String adj = getWordStr(tokens.get(depIdx - 1));
                        //if (adj != null) {
                        if (adj.endsWith("jj")) {
                            String compLabel = cntLabel(compCnt);
                            compCnt ++;
                            result += adj + " amod-" + compLabel + " ";
                            result_tail = " " + compLabel + result_tail;
                            phIs.remove(phIs.indexOf(depIdx));
                        }
                        //}
                    }
                }
            }
            result += noun + result_tail;
            return result;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: PhrasalSimilarity <pathToRuleFile>");
            System.exit(0);
        }

        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        BufferedReader ruleReader = new BufferedReader(new FileReader(args[0]));
        ruleReader.readLine();
        String line;
        String[] editor;
        String rule;
        String lPh, rPh;
        Annotation lSent, rSent;

        BufferedWriter writer1 = new BufferedWriter(new FileWriter(args[1] + "-rule"));
        BufferedWriter writer2 = new BufferedWriter(new FileWriter(args[1] + "-rule-comparison"));

        int cnt = 0;
        while ((rule = ruleReader.readLine()) != null) {
            editor = rule.split("\t");
            rule = editor[0];
            rule = rule.substring(1, rule.length() - 1);
            lPh = editor[1];
            rPh = editor[2];
            //System.out.println(cnt + "\t" + editor[1] + "\t\t" + editor[2]);

            /*
            if (cnt < 10) {
                System.out.println(rule + "\t" + lPh + "\t" + rPh + "\t" + lSent + "\t" + rSent);
            }
            */
            
            lSent = new Annotation(editor[9]);
            rSent = new Annotation(editor[10]);
            pipeline.annotate(lSent);
            pipeline.annotate(rSent);

            String lPhNew = ParsePhrase.parsePhrase(lPh, lSent);
            String rPhNew = ParsePhrase.parsePhrase(rPh, rSent);

/*
            System.out.print(String.valueOf(cnt + 1) + ":\t");
            System.out.print("\"" + lPh + "\" : \"" + lPhNew + "\"\t");
            System.out.print("\"" + rPh + "\" : \"" + rPhNew + "\"\n");
*/

            //writer1.write(String.valueOf(cnt + 1) + ":\t");
            writer1.write(lPhNew + "\t\t" + rPhNew + "\n");

            writer2.write(String.valueOf(cnt + 1) + ":\t");
            writer2.write("\"" + lPh + "\" : \"" + lPhNew + "\"\t");
            writer2.write("\"" + rPh + "\" : \"" + rPhNew + "\"\n");
            cnt++;
        }

        ruleReader.close();
        writer1.close();
        writer2.close();
    }

}

