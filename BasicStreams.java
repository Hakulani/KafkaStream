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


public class BasicStreams {

    public static final List<String> CHAPTER_NAMES = Arrays.asList(
            "THE BOY WHO LIVED",
            "THE VANASHIG GLASS",
            "THE LETTERS FROM NO ONE",
            "THE KEEPER OF THE KEYS",
            "DIAGON ALLY",
            "THE JOURNEY FROM PLATFORM NINE AND THREE-QUARTERS",
            "THE SORTING HAT",
            "THE POTIONS MASTER",
            "THE MIDNIGHT DUEL",
            "HALLOWEEN",
            "QUIDDITCH",
            "THE MIRROR OF ERISED",
            "NICHOLAS FLAMBL",
            "NORBERT THE NORWEGIAN RIDGEBACK",
            "THE FORBIDDEN FOREST",
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
        final String outputTopic = streamsProps.getProperty("basic.output.topic");

        // Serializers/deserializers (serde) for String and Long types
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

       KTable<String, Long> wordCounts = builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
       .peek((key, value) -> System.out.println("Incoming line - key " + key + " value " + value))
       .filterNot((key,value) -> value.length() < 1)  
       .filterNot((key, value) ->value.startsWith("Page |"))
       .map((key,value) -> KeyValue.pair(getKey(value), value))
       .peek((key,value) -> System.out.println("word " +key +" count " + value))
       //.peek((key, value) -> System.out.println("Before transform - key: " + key + ", value: " + value))
       //.transform(ChapterTransformer::new)

       .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
       //.peek((key, value) -> System.out.println("flatMapValues - key: " + key + ", value: " + value))
       .filter((key, value) -> !STOP_WORDS.contains(value) && value.matches("[a-zA-Z]+"))
       //.peek((key, value) -> System.out.println("filter - key: " + key + ", value: " + value))
       .groupBy((key, value) -> value, Grouped.with(stringSerde, stringSerde))
       .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("word-counts-store"));
       // Convert the `KTable<String, Long>` into a `KStream<String, Long>` and write to the output topic.
       wordCounts.toStream()
           .peek((key, value) -> System.out.println("Outgoing word - key " + key + " value " + value))
           .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        
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

    static class ChapterTransformer implements Transformer<String, String, KeyValue<String, String>> {

        private String currentChapterName;
        private boolean chapterNameDetected;
    
        @Override
        public void init(ProcessorContext context) {
            currentChapterName = "";
            chapterNameDetected = false;
        }
    
        @Override
        public KeyValue<String, String> transform(String key, String value) {
            if (CHAPTER_NAMES.contains(value.toUpperCase())) {
                currentChapterName = value;
                chapterNameDetected = true;
                return null; // Do not output anything when a chapter name is detected
            } else if (currentChapterName.isEmpty()) {
                return null; // Ignore lines before the first chapter name
            } else {
                if (chapterNameDetected) {
                    value = currentChapterName + ": " + value;
                    chapterNameDetected = false;
                }
                return KeyValue.pair(currentChapterName, value);
            }
        }
    
        @Override
        public void close() {
            // No resources to close in this example
        }
    }

}
