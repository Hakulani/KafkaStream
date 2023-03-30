package io.confluent.developer.basic;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import io.confluent.developer.basic.TopicLoader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.KeyValue;
import java.io.FileInputStream;
import java.io.IOException;
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

[วันอังคาร 23:15] สรวิชญ์ ศิลปนุรักษ์




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







[วันอังคาร 23:19] สุชาวลี จีระธัญญาสกุล




suchawalee.plan80@gmail.com




    public static void main(String[] args) throws IOException {
        Properties streamsProps = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            streamsProps.load(fis);
        }
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams");

        StreamsBuilder builder = new StreamsBuilder();

        final String inputTopic = streamsProps.getProperty("basic.input.topic");
        final String outputTopic = streamsProps.getProperty("basic.output.topic");

        KStream<String, String> firstStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .peek((key, value) -> System.out.println("Processing: " + value));

        // Add a currentChapter variable to keep track of the current chapter
        final String[] currentChapter = {null};

        Set<String> stopWords = new HashSet<>(Arrays.asList("a", "an", "the", "and", "but", "or", "if", "because", "as", "what", "when", "where", "how", "which", "who", "whom", "this", "that", "these", "those", "then", "just", "so", "than", "such", "both", "through", "about", "for", "while", "during", "before", "after", "since", "until", "although", "though", "even", "as", "whether", "if", "once", "unless", "until", "while"));

        
        KStream<String, Integer> wordCounts = firstStream.map((key, value) -> {
            String trimmedValue = value.trim();
            if (CHAPTER_NAMES.stream().anyMatch(chapter -> chapter.equalsIgnoreCase(trimmedValue))) {
                currentChapter[0] = trimmedValue;
                return new KeyValue<>(trimmedValue, 0);
            } else {
                String[] words = value.split("\\W+");
                int count = (int) Arrays.stream(words)
                        .filter(word -> !stopWords.contains(word.toLowerCase()))
                        .count();
                return new KeyValue<>(currentChapter[0], count);
            }
        });
               
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
        });

        KTable<String, Integer> aggregatedCounts1 = wordCounts.groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
            .reduce(Integer::sum);

        aggregatedCounts1.toStream()
            .peek((key, value) -> System.out.println(key + " --> count = " + value))
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Integer()));

        KTable<String, Integer> aggregatedCounts2 = potterCounts.groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
            .reduce(Integer::sum);

        aggregatedCounts2.toStream()
            .peek((key, value) -> System.out.println(key + " --> count = " + value))
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Integer()));

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

    public static boolean isChapterName(String s) {
        return CHAPTER_NAMES.contains(s);
    }
}
