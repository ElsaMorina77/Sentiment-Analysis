import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SentimentAnalysisParallel {
    public static void main(String[] args) {
        // Set up Stanford NLP code from the offical page
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        // AtomicIntegers counters
        AtomicInteger positiveCount = new AtomicInteger(0);
        AtomicInteger negativeCount = new AtomicInteger(0);
        AtomicInteger neutralCount = new AtomicInteger(0);
        AtomicInteger totalCount = new AtomicInteger(0);

        // Read the file with tweets in it
        String fileName = "C:\\Users\\DELL\\Desktop\\Twitter Consumer\\file.txt";

        // Make the thread pool
        int numThreads = Runtime.getRuntime().availableProcessors(); // Number of available CPU cores
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = br.readLine()) != null) {
                final String tweet = line;
                executorService.submit(() -> {
                    try {
                        // Analyze sentiment for each tweet
                        String sentiment = analyzeSentiment(tweet, pipeline);

                        // Count the sentiment
                        if ("Positive".equals(sentiment)) {
                            positiveCount.incrementAndGet();
                        } else if ("Negative".equals(sentiment)) {
                            negativeCount.incrementAndGet();
                        } else {
                            neutralCount.incrementAndGet();
                        }

                        totalCount.incrementAndGet();

                        // Print sentiment result for each tweet
                        //System.out.println("Sentiment for tweet: " + sentiment);
                        writeSentimentResultToFile(sentiment, "C:\\Users\\DELL\\Desktop\\Twitter Consumer\\sentiment_results_par.txt");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Shutdown the executor service
        executorService.shutdown();

        try {
            // Wait for all threads to finish
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

            // Calculate tweet rates
            double positiveRate = (double) positiveCount.get() / totalCount.get();
            double negativeRate = (double) negativeCount.get() / totalCount.get();
            double neutralRate = (double) neutralCount.get() / totalCount.get();

            // Write results to a file and print them to the console
            String resultsFilePath = "C:\\Users\\DELL\\Desktop\\Twitter Consumer\\results_for_par.txt";
            try (FileWriter writer = new FileWriter(resultsFilePath, true)) {
                String results = "Total Tweets: " + totalCount + "\n" +
                        "Positive Tweets: " + positiveCount + " (" + positiveRate * 100 + "%)\n" +
                        "Negative Tweets: " + negativeCount + " (" + negativeRate * 100 + "%)\n" +
                        "Neutral Tweets: " + neutralCount + " (" + neutralRate * 100 + "%)\n";

                System.out.println(results);
                writer.write(results);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Perform sentiment analysis on a single tweet
    private static String analyzeSentiment(String tweet, StanfordCoreNLP pipeline) {
        Annotation annotation = new Annotation(tweet);
        pipeline.annotate(annotation);

        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
            return sentiment;
        }

        return "Neutral";
    }

    // Write sentiment results to a file
    private static void writeSentimentResultToFile(String sentiment, String filePath) {
        try (FileWriter writer = new FileWriter(filePath, true)) {
            writer.write("Sentiment: " + sentiment + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
