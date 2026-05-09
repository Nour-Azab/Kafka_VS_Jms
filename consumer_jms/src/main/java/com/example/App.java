package com.example;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class App {

    public static void main(String[] args) throws Exception {

        String brokerURL = "tcp://localhost:61616";

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);

        Connection connection = factory.createConnection();

        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createQueue("lab-topic");

        MessageConsumer consumer = session.createConsumer(queue);

        List<Long> latencies = new ArrayList<>();

        int count = 0;
        List<Long> ResponseTimes = new ArrayList<>();

        while (count < 10000) {

            long startTime = System.currentTimeMillis();

            Message msg = consumer.receive(1000);

            long responseTime = System.currentTimeMillis() - startTime;

            ResponseTimes.add(responseTime);

            if (msg != null) {

                TextMessage textMessage = (TextMessage) msg;

                String value = textMessage.getText();

                long sentTime = Long.parseLong(value.split("\\|")[0]);

                long latency = System.currentTimeMillis() - sentTime;

                latencies.add(latency);

                System.out.println("Received message " + count);

                count++;
            }

            System.out.println(
                    "Receive operation took "
                            + responseTime + " ms");
        }

        Collections.sort(latencies);


        Collections.sort(ResponseTimes);
        System.out.println("Median Response Time: " + ResponseTimes.get(5000) + " ms");
        System.out.println(
                "Latency percentiles: 50th = "
                        + latencies.get(5000)
                        + " ms, 95th = "
                        + latencies.get(9500)
                        + " ms, 99th = "
                        + latencies.get(9900)
                        + " ms, " + "Messages received: " + count);

        consumer.close();
        session.close();
        connection.close();
    }
}