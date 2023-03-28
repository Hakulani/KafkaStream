package io.confluent.developer.basic;

import io.confluent.developer.basic.TopicLoader;
import io.confluent.developer.StreamsUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;


public class TopicLoader {

    public static void main(String[] args) throws IOException {
        runProducer();
    }

     public static void runProducer() throws IOException {
        Properties properties = StreamsUtils.loadProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try(Admin adminClient = Admin.create(properties);
            Producer<String, String> producer = new KafkaProducer<>(properties)) {
            final String inputTopic = "streams-plaintext2-input";
            final String outputTopic = "streams-wordcount2-output";
            var topics = List.of(StreamsUtils.createTopic(inputTopic), StreamsUtils.createTopic(outputTopic));
            adminClient.createTopics(topics);

            Callback callback = (metadata, exception) -> {
                if(exception != null) {
                    System.out.printf("Producing records encountered error %s %n", exception);
                } else {
                    System.out.printf("Record produced - offset - %d timestamp - %d %n", metadata.offset(), metadata.timestamp());
                }

            };

            var rawRecords = List.of("The quick brown fox jumps over the lazy dog.",
                                    "She sells seashells by the seashore.",
                                    "How much wood would a woodchuck chuck if a woodchuck could chuck wood?",
                                    "Peter Piper picked a peck of pickled peppers.",
                                    "I scream, you scream, we all scream for ice cream.",
                                    "To be or not to be, that is the question.",
                                    "It was the best of times, it was the worst of times.",
                                    "A tale of two cities.",
                                    "In the beginning God created the heavens and the earth.",
                                    "The cat in the hat comes back.",
                                    "a an and are as at be b");
            var producerRecords = rawRecords.stream().map(r -> new ProducerRecord<>(inputTopic,"totalWordCount", r)).collect(Collectors.toList());
            producerRecords.forEach((pr -> producer.send(pr, callback)));


        }
    }
}
