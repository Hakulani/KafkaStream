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
public class BasicStreams {

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


         //.filterNot((key,value) -> value.length() < 1)
       // .filterNot((key, value) ->value.startsWith("Page |"))
        //.filter((key, value) -> value.matches("[a-zA-Z]+"))     .filterNot((key, value) ->value.startsWith("Page |"))
        //    .filterNot((key,value) -> value.length() < 1)
        // Construct a `KStream` from the input topic "streams-plaintext-input", where message values
        // represent lines of text (for the sake of this example, we ignore whatever may be stored
        // in the message keys).

       // 

        KTable<String, Long> wordCounts = builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
        .peek((key, value) -> System.out.println("Incoming line - key " + key + " value " + value))
        .filterNot((key,value) -> value.length() < 1)  
        .filterNot((key, value) ->value.startsWith("Page |"))
        .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
        .filter((key, value) -> !STOP_WORDS.contains(value) && value.matches("[a-zA-Z]+"))
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
}
