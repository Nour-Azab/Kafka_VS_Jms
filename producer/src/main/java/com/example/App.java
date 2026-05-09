package com.example;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class App {
    void calculateResponseTime(Properties props) throws Exception {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // String payload = "K".repeat(1000);
        List<Long> ResponseTimes = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            long timeStamp = System.currentTimeMillis();
            String payload = timeStamp + "|" + "K".repeat(988);
            ProducerRecord<String, String> record = new ProducerRecord<>("lab-topic", "Key " + i, payload);
            long startTime = System.currentTimeMillis();
            producer.send(record).get();
            long responseTime = System.currentTimeMillis() - startTime;
            ResponseTimes.add(responseTime);
            //System.out.println("Response Time of msg " + i + ": " + responseTime + " ms");
        }
        Collections.sort(ResponseTimes);
        System.out.println("Median Response Time: " + ResponseTimes.get(500) + " ms");
        producer.close();
    }
    void calculateThroughput(Properties props, long messageCount) throws Exception {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        long startTime = System.currentTimeMillis();
        Double periodTime = 1 / (double)messageCount;
        for (int i = 0; i < messageCount; i++) {
            long timeStamp = System.currentTimeMillis();
            if(timeStamp - startTime > 1000) {
                break;
            }
            String payload = timeStamp + "|" + "K".repeat(988);
            ProducerRecord<String, String> record = new ProducerRecord<>("lab-topic", "Key " + i, payload);
            producer.send(record).get();
            //long responseTime = System.currentTimeMillis() - timeStamp;
            //System.out.println("Response Time of msg " + i + ": " + responseTime + " ms");
            //Thread.sleep((long) (0.8 * periodTime));
            
        }

        producer.close();
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        App app = new App();
        /*for (int i = 0; i < 1000; i++) {
            app.calculateResponseTime(props);
        }*/
        app.calculateResponseTime(props);
        //app.calculateThroughput(props, 1000);
    }
}