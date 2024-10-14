//import edu.stanford.nlp.ling.CoreAnnotations;
//import edu.stanford.nlp.pipeline.Annotation;
//import edu.stanford.nlp.pipeline.StanfordCoreNLP;
//import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
//import mpi.MPI;
//
//import java.io.*;
//import java.util.Properties;
//
//public class SentimentAnalysisDistributed {
//    public static void main(String[] args) throws Exception {
//        MPI.Init(args);
//        int rank = MPI.COMM_WORLD.Rank();
//        int size = MPI.COMM_WORLD.Size();
//        int tweetCount = 0;
//        long startTime = System.currentTimeMillis();
//
//        Properties props = new Properties();
//        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
//        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
//
//        String inputFileName = "5.txt";
//        String outputFileName = "sentiment_results_dis.txt";
//        String outputResultFile = "results_for_dis.txt";
//
//        try (BufferedReader br = new BufferedReader(new FileReader(inputFileName));
//             BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileName))) {
//            String line;
//            int tweetIndex = 0;
//
//            while ((line = br.readLine()) != null) {
//                // Distribute tweets using round-robin
//                if (tweetIndex % size == rank) {
//                    final String tweet = line;
//                    String sentiment = analyzeSentiment(tweet, pipeline);
//
//                    // Write sentiment result to the output file
//                    String result = "Tweet " + tweetIndex + ":\n" + tweet + "\nSentiment: " + sentiment + "\n\n";
//                    bw.write(result);
//
//                }
//                tweetIndex++;
//            }
//            tweetCount = tweetIndex;
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        long endTime = System.currentTimeMillis();
//        double totalTime = (endTime - startTime) / 1000.0;
//        double tweetsPerSecond = tweetCount / totalTime;
//        double averageTimePerTweet = totalTime / tweetCount;
//
//        // Print the calculation results to the result file and console
//        try (BufferedWriter resultWriter = new BufferedWriter(new FileWriter(outputResultFile, true))) {
//            String result = "Processor " + rank + ": Total Tweets: " + tweetCount +
//                    ", Tweets per Second: " + tweetsPerSecond + ", Average Time per Tweet: " + averageTimePerTweet + "s";
//            resultWriter.write(result);
//            resultWriter.newLine();
//            System.out.println(result);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        MPI.Finalize();
//    }
//
//    private static String analyzeSentiment(String tweet, StanfordCoreNLP pipeline) {
//        Annotation annotation = new Annotation(tweet);
//        pipeline.annotate(annotation);
//
//        for (edu.stanford.nlp.util.CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
//            String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
//            return sentiment;
//        }
//
//        return "Neutral";
//    }
//}
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import mpi.MPI;

import java.io.*;
import java.util.Properties;

public class SentimentAnalysisDistributed {
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();
        int tweetCount = 0;

        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        String inputFileName = "50000.txt";
        String outputFileName = "sentiment_results_dis.txt";
        String outputResultFile = "results_for_dis.txt";

        try (BufferedReader br = new BufferedReader(new FileReader(inputFileName));
             BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileName))) {
            String line;
            int tweetIndex = 0;

            while ((line = br.readLine()) != null) {
                // Distribute tweets using round-robin
                if (tweetIndex % size == rank) {
                    final String tweet = line;
                    String sentiment = analyzeSentiment(tweet, pipeline);
                    //System.out.println(tweet);
                    // Write sentiment result to the output file
                    String result = "Review " + tweetIndex + ":\n" + tweet + "\nSentiment: " + sentiment + "\n\n";
                    bw.write(result);
                }
                tweetIndex++;
            }
            tweetCount = tweetIndex;
        } catch (IOException e) {
            e.printStackTrace();
        }

        MPI.Finalize();

        long endTime = System.currentTimeMillis();
        double totalTime = (endTime - startTime) / 1000.0;
        double tweetsPerSecond = tweetCount / totalTime;
        double averageTimePerTweet = totalTime / tweetCount;

                                                                                                                      // Print the calculation results to the result file and console
        try (BufferedWriter resultWriter = new BufferedWriter(new FileWriter(outputResultFile, true))) {
            String result = "Total Reviews: " + tweetCount +
                    ", Total Time: " + totalTime + "s, Reviews per Second: " + tweetsPerSecond +
                    ", Average Time per Reviews: " + averageTimePerTweet + "s";
            resultWriter.write(result);
            resultWriter.newLine();
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String analyzeSentiment(String tweet, StanfordCoreNLP pipeline) {
        Annotation annotation = new Annotation(tweet);
        pipeline.annotate(annotation);

        for (edu.stanford.nlp.util.CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
            return sentiment;
        }

        return "Neutral";
    }
}
