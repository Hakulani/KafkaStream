package io.confluent.developer.basic;
import org.apache.kafka.common.utils.Bytes;
import io.confluent.developer.basic.TopicLoader;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.Set;
import java.util.HashSet;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.List;
import java.util.ArrayList;
import java.util.*;

public class BasicStreams {

    public static final List<String> CHAPTER_NAMES = Arrays.asList(
    "THE BOY WHO LIVED",
    "THE VANASHIG GLASS",
    "THE LETTERS FROM NO ONE",
    "THE KEEPER OF THE KEYS",
    "DIAGON ALLY",
    "THE JOURNEY FROM PLATFORM",
    "THE SORTING HAT",
    "THE POTIONS MASTER",
    "THE MIDNIGHT DUEL",
    "HALLOWEEN",
    "QUIDDITCH",
    "THE MIRROR OF ERISED",
    "NICHOLAS FLAMBL",
    "NORBERT THE NORWEGIAN",
    "THE FORBIDDEN FOREST",
    "THROUGH THE TRAPDOOR",
    "THE MAN WITH TWO FACES"
);


    static long i=0;
    public static String getKey(String  aa){


        if (aa.startsWith("THE BOY WHO LIVED"))
        {
            i = 1;
        }
        else if (aa.startsWith("THE VANISHING GLASS")){
            i = 2;
        }
        else if (aa.startsWith("THE LETTERS FROM NO ONE")){
            i = 3;
        }
        else if (aa.startsWith("THE KEEPER OF KEYS")){
            i = 4;
        }
        else if (aa.startsWith("DIAGON ALLEY")){
            i = 5;
        }
        else if (aa.startsWith("THE JOURNEY FROM PLATFORM")){
            i = 6;
        }
        else if (aa.startsWith("THE SORTING HAT")){
            i = 7;
        }
        else if (aa.startsWith("THE POTIONS MASTER")){
            i = 8;
        }
        else if (aa.startsWith("THE MIDNIGHT DUEL")){
            i = 9;
        }
        else if (aa.startsWith("HALLOWE'EN")){
            i = 10;
        }
        else if (aa.startsWith("QUIDDITCH")){
            i = 11;
        }
        else if (aa.startsWith("THE MIRROR OF ERISED")){
            i = 12;
        }
        else if (aa.startsWith("NICOLAS FLAMEL")){
            i = 13;
        }
        else if (aa.startsWith("NORBERT THE NORWEGIAN RIDGEBACK")){
            i = 14;
        }
        else if (aa.startsWith("THE FORBIDDEN FOREST")){
            i = 15;
        }
        else if (aa.startsWith("THROUGH THE TRAPDOOR")){
            i = 16;
        }
        else if (aa.startsWith("THE MAN WITH TWO FACES")){
            i = 17;
        }
        aa = "chapter" + Long.toString(i); //+ "|" +aa;
        return(aa);

    }



    public static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
        "a", "about", "above", "after", "again", "against", "ain", "all", "am", "an", "and", "any", 
        "are", "aren", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", 
        "between", "both", "but", "by", "can", "couldn", "couldn't", "d", "did", "didn", "didn't", 
        "do", "does", "doesn", "doesn't", "doing", "don", "don't", "down", "during", "each", "few",
         "for", "from", "further", "had", "hadn", "hadn't", "has", "hasn", "hasn't", "have", "haven", 
         "haven't", "having", "he", "her", "here", "hers", "herself", "him", "himself", "his", "how", "i",
          "if", "in", "into", "is", "isn", "isn't", "it", "it's", "its", "itself", "just", "ll", "m", "ma",
        "me", "mightn", "mightn't", "more", "most", "mustn", "mustn't", "my", "myself", "needn", "needn't",
        "no", "nor", "not", "now", "o", "of", "off", "on", "once", "only", "or", "other", "our", "ours",
        "ourselves", "out", "over", "own", "re", "s", "same", "shan", "shan't", "she", "she's", "should",
        "should've", "shouldn", "shouldn't", "so", "some", "such", "t", "than", "that", "that'll", "the",
        "their", "theirs", "them", "themselves", "then", "there", "these", "they", "this", "those", 
        "through", "to", "too", "under", "until", "up", "ve", "very", "was", "wasn", "wasn't", "we",
        "were", "weren", "weren't", "what", "when", "where", "which", "while", "who", "whom", "why", 
        "will", "with", "won", "won't", "wouldn", "wouldn't", "y", "you", "you'd", "you'll", "you're",
        "you've", "your", "yours", "yourself", "yourselves"));


    public static void main(String[] args) throws IOException {

    

        Properties streamsProps = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            streamsProps.load(fis);
        }
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams");

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("basic.input.topic");
        final String outputTopic1 = streamsProps.getProperty("basic.output.topic1");
        final String outputTopic2 = streamsProps.getProperty("basic.output.topic2");

        // Serializers/deserializers (serde) for String and Long types
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStream<String, String> firstStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .peek((key, value) -> System.out.println("In Value key " +key +"In Value: " + value))
            .filterNot((key,value) -> value.length() < 1)  
            .filterNot((key, value) ->value.startsWith("Page |"));
        
        final String[] currentChapter = {null};
//////////////////////////////////////////////////////////////////////
        KStream<String, Integer> wordCounts = firstStream.map((key, value) -> {
                String trimmedValue = value.trim();
                if (CHAPTER_NAMES.stream().anyMatch(chapter -> chapter.equalsIgnoreCase(trimmedValue))) {
                    currentChapter[0] = trimmedValue;
                    return new KeyValue<>(trimmedValue, 0);
                } else {

                    String[] words = value.split("\\W+");
                    int count = (int) Arrays.stream(words)
                            .filter(word -> !STOP_WORDS.contains(word.toLowerCase()))

                            .count();
                    return new KeyValue<>(currentChapter[0], count);
                }
            })
            
            .peek((key,value) -> System.out.println("Loop1 " +key +" value " + value))
            ;

        KStream<String, Integer> potterCounts = firstStream.map((key, value) -> {
            String trimmedValue = value.trim(); // Create a new variable for the trimmed value
            // Check if the value is a chapter name (case-insensitive) and update currentChapter accordingly
            if (CHAPTER_NAMES.stream().anyMatch(chapter -> chapter.equalsIgnoreCase(trimmedValue))) {
                currentChapter[0] = trimmedValue;
                return new KeyValue<>(trimmedValue, 0);
            } else {
                String[] sentences = value.split("(?<!\\w\\.\\w.)(?<![A-Z][a-z]\\.)(?<=\\.|\\?)\\s");
                int count = 0;
                for (String sentence : sentences) {
                    if (Pattern.compile("\\bpotter\\b", Pattern.CASE_INSENSITIVE).matcher(sentence).find()) {
                        count++;
                    }
                }
                // Use currentChapter as the key instead of null
                return new KeyValue<>(currentChapter[0], count);
            }
        })

        .peek((key,value) -> System.out.println("Loop2 " +key +" value " + value))

        ;
       

        KTable<String, Integer> aggregatedCounts1 = wordCounts.groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
            .reduce(Integer::sum);

        aggregatedCounts1.toStream()
            .peek((key, value) -> System.out.println("key agg1 ="+key + " --> count remove stopword = " + value))
            .to(outputTopic1, Produced.with(Serdes.String(), Serdes.Integer()));
 
        KTable<String, Integer> aggregatedCounts2 = potterCounts.groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
        .reduce(Integer::sum);

        aggregatedCounts2.toStream()
            .peek((key, value) -> System.out.println("key agg2"+key + " --> count include stopword = " + value))
            .to(outputTopic2, Produced.with(Serdes.String(), Serdes.Integer()));



        ////////////////////////////////////////////////////////////////////

        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
            TopicLoader.runProducer();
            try {
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
}

   

}
