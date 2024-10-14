import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class SentimentAnalysisSequential {
    public static void main(String[] args) {
        // Set up Stanford NLP code from the official site
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        // Initialize FileWriter for sentiment results
        FileWriter sentimentResultWriter = null;

        // counters
        int positiveCount = 0;
        int negativeCount = 0;
        int neutralCount = 0;
        int totalCount = 0;

        // Record start time
        long startTime = System.currentTimeMillis();

        // Read the file
        String fileName = "C:\\Users\\DELL\\Desktop\\Twitter Consumer\\20000.txt";
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            // Initialize the sentiment results file
            sentimentResultWriter = new FileWriter("C:\\Users\\DELL\\Desktop\\Twitter Consumer\\sentiment_results_seq.txt", true);

            while ((line = br.readLine()) != null) {
                // Analyze sentiment for each tweet
                String sentiment = analyzeSentiment(line, pipeline);
                // System.out.println("Sentiment: " + sentiment);

                // Write sentiment result to the file
                sentimentResultWriter.write("Sentiment: " + sentiment + "\n");

                // Count the sentiment
                if ("Positive".equals(sentiment)) {
                    positiveCount++;
                } else if ("Negative".equals(sentiment)) {
                    negativeCount++;
                } else {
                    neutralCount++;
                }

                totalCount++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // Close the sentiment results file / cleanup
            if (sentimentResultWriter != null) {
                try {
                    sentimentResultWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // Record end time
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        // Calculate rev rates
        double positiveRate = (double) positiveCount / totalCount;
        double negativeRate = (double) negativeCount / totalCount;
        double neutralRate = (double) neutralCount / totalCount;

        // Print sentiment counts, rates, and time
        System.out.println("Total Reviews: " + totalCount);
        System.out.println("Positive Reviews: " + positiveCount + " (" + positiveRate * 100 + "%)");
        System.out.println("Negative Reviews: " + negativeCount + " (" + negativeRate * 100 + "%)");
        System.out.println("Neutral Reviews: " + neutralCount + " (" + neutralRate * 100 + "%)");
        System.out.println("Time taken: " + totalTime + " milliseconds");

        // Write results to a file
        try (FileWriter writer = new FileWriter("C:\\Users\\DELL\\Desktop\\Twitter Consumer\\results_for_seq.txt", true)) {
            writer.write("Total Reviews: " + totalCount + "\n");
            writer.write("Positive Reviews: " + positiveCount + " (" + positiveRate * 100 + "%)\n");
            writer.write("Negative Reviews: " + negativeCount + " (" + negativeRate * 100 + "%)\n");
            writer.write("Neutral Reviews: " + neutralCount + " (" + neutralRate * 100 + "%)\n");
            writer.write("Time taken: " + totalTime + " milliseconds\n");
        } catch (IOException e) {
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
}
