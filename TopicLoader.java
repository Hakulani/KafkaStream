package io.confluent.developer.basic;

import io.confluent.developer.StreamsUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.KStream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.streams.kstream.KTable;

public class TopicLoader {

    public static void main(String[] args) throws IOException {
        runProducer();
    }

    /**
     * @throws IOException
     */
    public static void runProducer() throws IOException {
        Properties properties = StreamsUtils.loadProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put("acks", "all");
        properties.put("replication.factor", "1");

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

            var is_break = true;
            String fileName = "D:\\DADS6005\\Quiz\\learn-kafka-courses\\learn-kafka-courses\\kafka-streams\\src\\main\\resources\\book.txt";
            List<String> stringList = new ArrayList<>();
            StringBuilder sb = new StringBuilder();

            HashMap<String, ArrayList<String>> map = new HashMap<>();
            String currentKey = null;
            ArrayList<String> currentList = null;
            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                String line;
                int beforeEmptyLineCount = 0;
                int afterEmptyLineCount = 0;
                boolean chapterNameFound = false;
                String possibleChapterName = null;
    
                while ((line = br.readLine()) != null) {
                    String tline = line.trim();
                    if (tline.isEmpty()) {
                        if (chapterNameFound) {
                            afterEmptyLineCount++;
                        } else {
                            beforeEmptyLineCount++;
                        }
                    } else {
                        if (chapterNameFound && afterEmptyLineCount >= 1) {
                            chapterNameFound = false;
                        }
                        String combinedChapterName = possibleChapterName != null ? possibleChapterName + " " + tline : tline;

                    
                        if (combinedChapterName.matches("^[A-Z\\s]+$")&&combinedChapterName.length() > 5 && beforeEmptyLineCount >= 3) {
                            // If all characters in the line are in uppercase, add it as a single string to the list
                            if (sb.length() > 0) {
                                stringList.add(sb.toString().trim());
        
                                sb = new StringBuilder();
                            }
        
                            stringList.add(combinedChapterName);
                            currentKey = combinedChapterName;
                            currentList = new ArrayList<>();
                            map.put(currentKey, currentList);
                            chapterNameFound = true;
                            afterEmptyLineCount = 0;
                            possibleChapterName = null;

                        }   else if (!tline.isEmpty() && !tline.equals("\n") && !tline.equals(" ") && !tline.equals("\t") && !tline.equals("\r") && currentList != null) {
                            // Append the line to the StringBuilder
        
                                sb.append(line);
                                if (sb.length() > 0) {
                                    currentList.add(line);
                                    }
                                beforeEmptyLineCount = 0;
                                possibleChapterName = tline.matches("^[A-Z\\s]+$") ? tline : null;

                        }   else {  beforeEmptyLineCount = 0;
                                    possibleChapterName = null;
                                }

                    }
                
              }
            } catch (IOException e) {
                e.printStackTrace();
            }


            HashMap<String, String> concatenatedMap = new HashMap<>();
            for (Map.Entry<String, ArrayList<String>> entry : map.entrySet()) {
                String key = entry.getKey();
                ArrayList<String> valueList = entry.getValue();
                String concatenatedString = String.join("", valueList);
                concatenatedMap.put(key, concatenatedString);
            }

            //for (String key : concatenatedMap.keySet()) {
            //    System.out.println("Key:"+key+ " "+ "Value:" + concatenatedMap.get(key));
            //}
            for (String key : concatenatedMap.keySet()) {
                ProducerRecord<String, String> record = new ProducerRecord<>("inputTopic", key, concatenatedMap.get(key).toString());
                producer.send(record);
            }
        }
    }
  }  //
