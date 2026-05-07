package com.example;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class App {
    public static void main(String[] args) throws Exception {

        String brokerURL = "tcp://localhost:61616";

        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory(brokerURL);

        Connection connection = factory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createQueue("lab-topic");

        MessageProducer producer = session.createProducer(queue);

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        
        long medianResponseTime = 0;

        for (int i = 0; i < 1000; i++) {

            long timestamp = System.currentTimeMillis();

            String payload = timestamp + "|" + "K".repeat(988);

            TextMessage message = session.createTextMessage(payload);

            long startTime = System.currentTimeMillis();

            producer.send(message);

            long responseTime = System.currentTimeMillis() - startTime;

            medianResponseTime += responseTime;

            System.out.println("Response Time of msg " + i +
                    ": " + responseTime + " ms");
        }
        medianResponseTime /= 1000;
        System.out.println("Median Response Time: " + medianResponseTime + " ms");

        producer.close();
        session.close();
        connection.close();
    }
}