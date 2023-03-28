package org.example;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.kstream.Produced;


public class Main {

    String[] stopword = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you",
            "you're", "you've", "you'll", "you'd", "your", "yours", "yourself",
            "yourselves", "he", "him", "his", "himself", "she", "she's", "her",
            "hers", "herself", "it", "it's", "its", "itself", "they", "them",
            "their", "theirs", "themselves", "what", "which", "who", "whom",
            "this", "that", "that'll", "these", "those", "am", "is", "are", "was",
            "were", "be", "been", "being", "have", "has", "had", "having", "do", "does",
            "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as",
            "until", "while", "of", "at", "by", "for", "with", "about", "against",
            "between", "into", "through", "during", "before", "after", "above",
            "below", "to", "from", "up", "down", "in", "out", "on", "off", "over",
            "under", "again", "further", "then", "once", "here", "there", "when",
            "where", "why", "how", "all", "any", "both", "each", "few", "more", "most",
            "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so",
            "than", "too", "very", "s", "t", "can", "will", "just", "don", "don't",
            "should", "should've", "now", "d", "ll", "m", "o", "re", "ve", "y", "ain",
            "aren", "aren't", "couldn", "couldn't", "didn", "didn't", "doesn", "doesn't",
            "hadn", "hadn't", "hasn", "hasn't", "haven", "haven't", "isn", "isn't", "ma",
            "mightn", "mightn't", "mustn", "mustn't", "needn", "needn't",
            "shan", "shan't", "shouldn", "shouldn't", "wasn", "wasn't",
            "weren", "weren't",
            "won", "won't", "wouldn", "wouldn't"};
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
    public Topology createTopology(){
        final Serdes.StringSerde stringSerde = new Serdes.StringSerde();
        //final Serdes.LongSerde longSerde = new Serdes.LongSerde();
        StreamsBuilder builder = new StreamsBuilder();
        // 1 - stream from Kafka

        KStream<String, String> textLines = builder.stream("raw.data.two");
        KStream<String, String> output = textLines.filterNot((key,value) -> value.startsWith("Page |"))
                .filterNot((key,value) -> value.length() < 1)
                .map((key,value) -> KeyValue.pair(getKey(value), value))
                .peek((key,value) -> System.out.println("word " +key +" count " + value));


        output.to("by.chapter.two", Produced.with(stringSerde, stringSerde));

        /*
        KStream<String, String> mo = textLines.peek(
                (key,value) -> {
                    //System.out.println(key + " => " + value);
                    value = value + "Test";
                    //System.out.println("word " +key +" count " + value);
                }
        );
        */
        //KStream<String, String> aa = textLines.filter((key, value) -> value.equals(value.toUpperCase()) == true);
        //aa.peek((key,value) -> System.out.println("word " +key +" count " + value));


        /*
        KTable<String, Long> wordCounts = textLines
                // 2 - map values to lowercase
                .mapValues(textLine -> textLine.toLowerCase())
                // can be alternatively written as:
                // .mapValues(String::toLowerCase)
                // 3 - flatmap values split by space
                .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
                // 4 - select key to apply a key (we discard the old key)
                .selectKey((key, word) -> word)
                // 5 - group by key before aggregation
                .groupByKey()
                // 6 - count occurences
                .count(Materialized.as("Counts"));
        */

        // 7 - to in order to write the results back to kafka
        //wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        //wordCounts.toStream().peek((key,value) -> System.out.println("word " +key +" count " + value));

        return builder.build();
    }
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "Test000");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "ec2-54-169-163-88.ap-southeast-1.compute.amazonaws.com:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //config.put(ConsumerConfig., "earliest");
        //config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        System.out.println("Hello world!");

        Main wordCountApp = new Main();

        KafkaStreams streams = new KafkaStreams(wordCountApp.createTopology(), config);
        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Update:
        // print the topology every 10 seconds for learning purposes
        while(true){
            streams.localThreadsMetadata().forEach(data -> System.out.println(data));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}