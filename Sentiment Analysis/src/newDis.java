

import mpi.MPI;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;

public class newDis {
    private static int totalTweets;

    public static void main(String[] args) throws IOException {
        double totalAnalysisStart;
        double totalAnalysisEnd;
        double totalAnalysis;
        MPI.Init(args);

        int rank = MPI.COMM_WORLD.Rank();

        totalAnalysisStart = System.currentTimeMillis();

        if (rank == 0) {
            masterProcess();
        } else {
            workerProcess();
        }

        totalAnalysisEnd = System.currentTimeMillis();

        MPI.COMM_WORLD.Barrier(); // Ensure all processes reach this point before finalizing

        if (rank == 0) {
            double largestDouble = findMax();
            System.out.println("\n\n\nThe time of the slowest worker is: " + largestDouble);
            System.out.println("\nTweet per second for the whole program: " + largestDouble / totalTweets);

            double averagePipeline = calculateAverageFromFile("baba.txt");
            System.out.println("\nAverage time to get the pipeline is: " + averagePipeline);

            int numOfTweets = countLines("5.txt");
            System.out.println("\nNumber of tweets we analyze: " + numOfTweets);
        } else {
        }

        MPI.Finalize();
    }


    private static void masterProcess() throws IOException {
        try {
            BufferedWriter sentimentWriter = new BufferedWriter(new FileWriter("baba.txt"));
            sentimentWriter.write("");
            sentimentWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            BufferedWriter sentimentWriter = new BufferedWriter(new FileWriter("baba.txt"));
            sentimentWriter.write("");
            sentimentWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            BufferedWriter sentimentWriter = new BufferedWriter(new FileWriter("baba.txt"));
            sentimentWriter.write("");
            sentimentWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        int numWorkers = MPI.COMM_WORLD.Size() - 1;

        String filename = "5.txt";

        BufferedReader reader = new BufferedReader(new FileReader(filename));

        String line;

        StringBuilder sb = new StringBuilder();

        while ((line = reader.readLine()) != null) {
            sb.append(line).append("\n");
        }
        reader.close();

        String[] tweets = sb.toString().split("\n");
        int numTweets = tweets.length;
        totalTweets = numTweets;

        int tweetsPerWorker = numTweets / numWorkers;
        int remainingTweets = numTweets % numWorkers;

        for (int i = 1; i <= numWorkers; i++) {
            int startIndex = (i - 1) * tweetsPerWorker;
            int endIndex = startIndex + tweetsPerWorker;

            if (i == numWorkers) {
                endIndex += remainingTweets;
            }

            int currentTweetsPerWorker = endIndex - startIndex;

            String[] workerTweets = new String[currentTweetsPerWorker];
            System.arraycopy(tweets, startIndex, workerTweets, 0, currentTweetsPerWorker);

            MPI.COMM_WORLD.Send(new int[]{currentTweetsPerWorker}, 0, 1, MPI.INT, i, 0);
            MPI.COMM_WORLD.Send(workerTweets, 0, workerTweets.length, MPI.OBJECT, i, 1);
        }
    }

    private static void workerProcess() throws IOException {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("baba.txt", true));

            int[] numTweets = new int[1];
            MPI.COMM_WORLD.Recv(numTweets, 0, 1, MPI.INT, 0, 0);

            String[] tweets = new String[numTweets[0]];
            MPI.COMM_WORLD.Recv(tweets, 0, numTweets[0], MPI.OBJECT, 0, 1);

            Properties props = new Properties();
            props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
            StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

            double timerCreatePipelineStart = System.currentTimeMillis();

            try {
                for (int i = 0; i < tweets.length; i++) {
                    String tweet = tweets[i];
                    Annotation annotation = new Annotation(tweet);
                    pipeline.annotate(annotation);

                    String sentiment = annotation.get(CoreAnnotations.SentencesAnnotation.class).get(0)
                            .get(SentimentCoreAnnotations.SentimentClass.class);

                    writer.write("" + sentiment);
                    writer.newLine();

                    System.out.println("Processor: " + MPI.COMM_WORLD.Rank());
                    System.out.println("Tweet " + i + ": " + tweet + "\nSentiment: " + sentiment);
                }
            } finally {
                writer.close();

                double timerCreatePipelineEnd = System.currentTimeMillis();
                MPI.COMM_WORLD.Send(new double[]{timerCreatePipelineEnd - timerCreatePipelineStart}, 0, 1, MPI.DOUBLE, 0, 2);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static double findMax() {
        double maxTime = 0.0;
        double[] workerTimes = new double[MPI.COMM_WORLD.Size() - 1];

        MPI.COMM_WORLD.Gather(new double[]{0.0}, 0, 1, MPI.DOUBLE, workerTimes, 0, 1, MPI.DOUBLE, 0);

        for (double time : workerTimes) {
            if (time > maxTime) {
                maxTime = time;
            }
        }

        return maxTime;
    }

    private static double calculateAverageFromFile(String fileName) {
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            double sum = 0.0;
            int count = 0;

            String line;
            while ((line = reader.readLine()) != null) {
                sum += Double.parseDouble(line);
                count++;
            }

            return count > 0 ? sum / count : 0.0;
        } catch (IOException e) {
            e.printStackTrace();
            return 0.0;
        }
    }

    private static int countLines(String fileName) {
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            int count = 0;
            while (reader.readLine() != null) {
                count++;
            }
            return count;
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }
}
