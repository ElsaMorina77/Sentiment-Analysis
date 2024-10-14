//import edu.stanford.nlp.ling.CoreAnnotations;
//import edu.stanford.nlp.pipeline.Annotation;
//import edu.stanford.nlp.pipeline.StanfordCoreNLP;
//import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
//import mpi.MPI;
//
//import java.io.*;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Properties;
//
//public class Distributed {
//    public static void main(String[] args) throws Exception {
//        MPI.Init(args);
//        int rank = MPI.COMM_WORLD.Rank();
//        int size = MPI.COMM_WORLD.Size();
//
//        if (rank == 0) {
//            // Master process
//            masterProcess(size);
//        } else {
//            // Worker processes
//            workerProcess(rank);
//        }
//
//        MPI.Finalize();
//    }
//
////    private static void masterProcess(int numWorkers) throws IOException {
////        // Read input file and split data into segments
////        String inputFileName = "prova.txt";
////        String outputFileName = "sentiment_results_dis.txt";
////        String outputResultFile = "results_for_dis.txt";
////
////        List<String> segments = readAndSplitInput(inputFileName, numWorkers);
////        int totalTweets = 0;
////
////        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileName, true))) {
////            for (int i = 1; i < numWorkers; i++) {
////                // Distribute segments to worker processes
////                int segmentSize = segments.size() / numWorkers;
////                int startIndex = (i - 1) * segmentSize;
////                int endIndex;
////                if (i == numWorkers - 1) {
////                    endIndex = segments.size();
////                } else {
////                    endIndex = startIndex + segmentSize;
////                }
////
////                List<String> segment = segments.subList(startIndex, endIndex);
////
////                // Send segment size and data to workers
////                MPI.COMM_WORLD.Send(new int[]{segment.size()}, 0, 1, MPI.INT, i, 0);
////                MPI.COMM_WORLD.Send(segment.toArray(new String[0]), 0, segment.size(), MPI.OBJECT, i, 1);
////            }
////
////
////            // Receive results from workers
////            // Receive results from workers
////            for (int i = 1; i < numWorkers; i++) {
////                int[] resultSize = new int[1];
////                MPI.COMM_WORLD.Recv(resultSize, 0, 1, MPI.INT, i, 2);
////
////                String[] results = new String[resultSize[0]];
////                MPI.COMM_WORLD.Recv(results, 0, resultSize[0], MPI.OBJECT, i, 3);
////
////                // Write results to the output file
////                for (String result : results) {
////                    bw.write(result);
////                    bw.newLine();
////                }
////
////                totalTweets += results.length;
////            }
////        }
////        // Calculate and report metrics
////        double totalTime = System.currentTimeMillis() / 1000.0;
////        double tweetsPerSecond = totalTweets / totalTime;
////        double averageTimePerTweet = totalTime / totalTweets;
////
////        try (BufferedWriter resultWriter = new BufferedWriter(new FileWriter(outputResultFile, true))) {
////            String result = "Master Process: Total Tweets: " + totalTweets +
////                    ", Tweets per Second: " + tweetsPerSecond + ", Average Time per Tweet: " + averageTimePerTweet + "s";
////            resultWriter.write(result);
////            resultWriter.newLine();
////            System.out.println(result);
////        } catch (IOException e) {
////            e.printStackTrace();
////        }
////    }
//
//    private static void masterProcess(int numWorkers) throws IOException {
//        // Read input file and split data into segments
//        String inputFileName = "prova.txt";
//        String outputFileName = "sentiment_results_dis.txt";
//        String outputResultFile = "results_for_dis.txt";
//
//        List<String> segments = readAndSplitInput(inputFileName, numWorkers);
//        int totalTweets = 0;
//
//        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileName, true))) {
//            for (int i = 1; i < numWorkers; i++) {
//                // Distribute segments to worker processes
//                int segmentSize = segments.size() / numWorkers;
//                int remainder = segments.size() % numWorkers;
//
//                int startIndex = (i - 1) * segmentSize + Math.min(i - 1, remainder);
//                int endIndex = i * segmentSize + Math.min(i, remainder);
//
//                List<String> segment = segments.subList(startIndex, endIndex);
//
//                // Send segment size and data to workers
//                MPI.COMM_WORLD.Send(new int[]{segment.size()}, 0, 1, MPI.INT, i, 0);
//                MPI.COMM_WORLD.Send(segment.toArray(new String[0]), 0, segment.size(), MPI.OBJECT, i, 1);
//            }
//
//            // Send the remaining tweets to the last processor
//            int lastSegmentSize = segments.size() - (numWorkers - 1) * (segments.size() / numWorkers);
//            List<String> lastSegment = segments.subList((numWorkers - 1) * (segments.size() / numWorkers), segments.size());
//
//            MPI.COMM_WORLD.Send(new int[]{lastSegment.size()}, 0, 1, MPI.INT, numWorkers - 1, 0);
//            MPI.COMM_WORLD.Send(lastSegment.toArray(new String[0]), 0, lastSegment.size(), MPI.OBJECT, numWorkers - 1, 1);
//
//            // Receive results from workers
//            for (int i = 1; i < numWorkers; i++) {
//                int[] resultSize = new int[1];
//                MPI.COMM_WORLD.Recv(resultSize, 0, 1, MPI.INT, i, 2);
//
//                String[] results = new String[resultSize[0]];
//                MPI.COMM_WORLD.Recv(results, 0, resultSize[0], MPI.OBJECT, i, 3);
//
//                // Write results to the output file
//                for (String result : results) {
//                    bw.write(result);
//                    bw.newLine();
//                }
//
//                totalTweets += results.length;
//            }
//        }
//
//        // Calculate and report metrics
//        double totalTime = System.currentTimeMillis() / 1000.0;
//        double tweetsPerSecond = totalTweets / totalTime;
//        double averageTimePerTweet = totalTime / totalTweets;
//
//        try (BufferedWriter resultWriter = new BufferedWriter(new FileWriter(outputResultFile, true))) {
//            String result = "Master Process: Total Tweets: " + totalTweets +
//                    ", Tweets per Second: " + tweetsPerSecond + ", Average Time per Tweet: " + averageTimePerTweet + "s";
//            resultWriter.write(result);
//            resultWriter.newLine();
//            System.out.println(result);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//
//    private static void workerProcess(int rank) throws IOException {
//        // Receive segment size and data from the master
//        int[] segmentSize = new int[1];
//        MPI.COMM_WORLD.Recv(segmentSize, 0, 1, MPI.INT, 0, 0);
//
//        String[] segment = new String[segmentSize[0]];
//        MPI.COMM_WORLD.Recv(segment, 0, segmentSize[0], MPI.OBJECT, 0, 1);
//
//        // Set up Stanford NLP
//        Properties props = new Properties();
//        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
//        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
//
//        // Analyze sentiments for the segment
//        String[] results = analyzeSentiments(segment, pipeline);
//
//        // Send results back to the master
//        MPI.COMM_WORLD.Send(new int[]{results.length}, 0, 1, MPI.INT, 0, 2);
//        MPI.COMM_WORLD.Send(results, 0, results.length, MPI.OBJECT, 0, 3);
//    }
//
//    private static List<String> readAndSplitInput(String inputFileName, int numWorkers) throws IOException {
//        List<String> segments = new ArrayList<>();
//        try (BufferedReader br = new BufferedReader(new FileReader(inputFileName))) {
//            String line;
//            StringBuilder segment = new StringBuilder();
//            int lineCount = 0;
//
//            while ((line = br.readLine()) != null) {
//                segment.append(line).append("\n");
//                lineCount++;
//
//                if (lineCount >= numWorkers) {
//                    segments.add(segment.toString());
//                    segment.setLength(0);
//                    lineCount = 0;
//                }
//            }
//
//            // Add any remaining lines to the last segment
//            if (segment.length() > 0) {
//                segments.add(segment.toString());
//            }
//        }
//
//        return segments;
//    }
//
//    private static String[] analyzeSentiments(String[] tweets, StanfordCoreNLP pipeline) {
//        List<String> results = new ArrayList<>();
//        for (int i = 0; i < tweets.length; i++) {
//            String tweet = tweets[i];
//            Annotation annotation = new Annotation(tweet);
//            pipeline.annotate(annotation);
//
//            for (edu.stanford.nlp.util.CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
//                String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
//                results.add("Tweet " + i + ":\n" + tweet + "\nSentiment: " + sentiment);
//            }
//        }
//        return results.toArray(new String[0]);
//    }
//}
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import mpi.MPI;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Distributed {
    public static void main(String[] args) throws Exception {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();

        if (rank == 0) {
            // Master process
            masterProcess(size);
        } else {
            // Worker processes
            workerProcess(rank);
        }

        MPI.Finalize();
    }

    private static void masterProcess(int numWorkers) throws IOException {
        // Read input file and split data into segments
        String inputFileName = "file.txt";
        String outputFileName = "baba.txt";
        String outputResultFile = "results_for_dis.txt";

        List<String> segments = readAndSplitInput(inputFileName, numWorkers);
        int totalTweets = 0;

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileName, true))) {
            // Use MPI Barrier to synchronize all processes before distributing segments
            MPI.COMM_WORLD.Barrier();

            for (int i = 1; i < numWorkers; i++) {
                // Distribute segments to worker processes
                int startIndex = (i - 1) * (segments.size() / (numWorkers - 1));
                int endIndex = i * (segments.size() / (numWorkers - 1));

                List<String> segment = segments.subList(startIndex, endIndex);

                // Send segment size and data to workers
                MPI.COMM_WORLD.Send(new int[]{segment.size()}, 0, 1, MPI.INT, i, 0);
                MPI.COMM_WORLD.Send(segment.toArray(new String[0]), 0, segment.size(), MPI.OBJECT, i, 1);
            }

            // Receive results from workers
            for (int i = 1; i < numWorkers; i++) {
                int[] resultSize = new int[1];
                MPI.COMM_WORLD.Recv(resultSize, 0, 1, MPI.INT, i, 2);

                String[] results = new String[resultSize[0]];
                MPI.COMM_WORLD.Recv(results, 0, resultSize[0], MPI.OBJECT, i, 3);

                // Write results to the output file
                for (String result : results) {
                    bw.write(result);
                    bw.newLine();
                }

                totalTweets += results.length;
            }
        }

        // Calculate and report metrics
        double totalTime = System.currentTimeMillis() / 1000.0;
        double tweetsPerSecond = totalTweets / totalTime;
        double averageTimePerTweet = totalTime / totalTweets;

        try (BufferedWriter resultWriter = new BufferedWriter(new FileWriter(outputResultFile, true))) {
            String result = "Master Process: Total Tweets: " + totalTweets +
                    ", Tweets per Second: " + tweetsPerSecond + ", Average Time per Tweet: " + averageTimePerTweet + "s";
            resultWriter.write(result);
            resultWriter.newLine();
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void workerProcess(int rank) throws IOException {
        // Use MPI Barrier to synchronize all processes before receiving segments
        MPI.COMM_WORLD.Barrier();

        // Receive segment size and data from the master
        int[] segmentSize = new int[1];
        MPI.COMM_WORLD.Recv(segmentSize, 0, 1, MPI.INT, 0, 0);

        String[] segment = new String[segmentSize[0]];
        MPI.COMM_WORLD.Recv(segment, 0, segmentSize[0], MPI.OBJECT, 0, 1);

        // Set up Stanford NLP
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        // Analyze sentiments for the segment
        String[] results = analyzeSentiments(segment, pipeline);

        // Send results back to the master
        MPI.COMM_WORLD.Send(new int[]{results.length}, 0, 1, MPI.INT, 0, 2);
        MPI.COMM_WORLD.Send(results, 0, results.length, MPI.OBJECT, 0, 3);
    }
    private static List<String> segmentToStringList(List<String> segment) {
        return new ArrayList<>(segment);
    }


    private static List<String> readAndSplitInput(String inputFileName, int numWorkers) throws IOException {
        List<String> segments = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(inputFileName))) {
            List<String> tweets = new ArrayList<>();
            String line;

            while ((line = br.readLine()) != null) {
                tweets.add(line);
            }

            int tweetsPerSegment = tweets.size() / numWorkers;
            int remainingTweets = tweets.size() % numWorkers;

            int startIndex = 0;
            int endIndex;

            for (int i = 0; i < numWorkers; i++) {
                endIndex = startIndex + tweetsPerSegment + (i < remainingTweets ? 1 : 0);

                List<String> segmentList = tweets.subList(startIndex, endIndex);
                segments.add(String.join("\n", segmentList));

                startIndex = endIndex;
            }
        }

        return segments;
    }


    private static List<String> segmentToStringList(String segment) {
        List<String> result = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new StringReader(segment))) {
            String line;
            while ((line = reader.readLine()) != null) {
                result.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    private static String[] analyzeSentiments(String[] tweets, StanfordCoreNLP pipeline) {
        List<String> results = new ArrayList<>();
        for (int i = 0; i < tweets.length; i++) {
            String tweet = tweets[i];
            Annotation annotation = new Annotation(tweet);
            pipeline.annotate(annotation);

            for (edu.stanford.nlp.util.CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
                results.add("Tweet " + i + ":\n" + tweet + "\nSentiment: " + sentiment);
            }
        }
        return results.toArray(new String[0]);
    }
}