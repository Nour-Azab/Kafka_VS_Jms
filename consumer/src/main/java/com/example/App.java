package com.example;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group-1");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("lab-topic"));

        List<Long> latencies = new ArrayList<>();
        int count = 0;
        long medianResponseTime = 0;
        while (count < 10000) {
            long startTime = System.currentTimeMillis();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            long responseTime = System.currentTimeMillis() - startTime;
            medianResponseTime += responseTime;

            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                long sentTime = Long.parseLong(value.split("\\|")[0]);
                long latency = System.currentTimeMillis() - sentTime;
                latencies.add(latency);
                System.out.println("Received message: key = " + record.key()
                        + ", partition = " + record.partition() + ", offset = " + record.offset());
                count++;
            }
            System.out.println("Polled " + records.count() + " records in " + responseTime + " ms");
        }

        Collections.sort(latencies);
        System.out.println("Latency percentiles: 50th = " + latencies.get(500) + " ms, 95th = " + latencies.get(950)
                + " ms, 99th = " + latencies.get(990) + " ms, " + "Messages received: " + count);
        System.out.println("Median Response Time: " + medianResponseTime / 1000 + " ms");
        consumer.close();
    }
}
