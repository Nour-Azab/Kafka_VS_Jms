package com.example;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.activemq.ActiveMQConnectionFactory;

public class App {

    void calculateResponseTime() throws Exception {

        String brokerURL = "tcp://localhost:61616";

        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory(brokerURL);

        Connection connection = factory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createQueue("lab-topic");

        MessageProducer producer = session.createProducer(queue);

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        
        List<Long> ResponseTimes = new ArrayList<>();

        for (int i = 0; i < 10000; i++) {

            long timestamp = System.currentTimeMillis();

            String payload = timestamp + "|" + "K".repeat(988);

            TextMessage message = session.createTextMessage(payload);

            long startTime = System.currentTimeMillis();

            producer.send(message);

            long responseTime = System.currentTimeMillis() - startTime;

            ResponseTimes.add(responseTime);

           // System.out.println("Response Time of msg " + i +": " + responseTime + " ms");
        }
        Collections.sort(ResponseTimes);
        System.out.println("Median Response Time: " + ResponseTimes.get(500) + " ms");

        producer.close();
        session.close();
        connection.close();
    }

    void calculateThroughput(long messageCount) throws Exception {
        String brokerURL = "tcp://localhost:61616";

        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory(brokerURL);

        Connection connection = factory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createQueue("lab-topic");

        MessageProducer producer = session.createProducer(queue);

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < messageCount; i++) {
            long timeStamp = System.currentTimeMillis();
            if(timeStamp - startTime > 1000) {
                break;
            }
            String payload = timeStamp + "|" + "K".repeat(988);
            TextMessage message = session.createTextMessage(payload);
            producer.send(message);
        }
        producer.close();
        session.close();
        connection.close();

    }

    public static void main(String[] args) throws Exception {
        App app = new App();
        /*for (int i = 0; i < 1000; i++) {
            app.calculateResponseTime();
        }*/
        app.calculateResponseTime();
        //app.calculateThroughput(1000);

    }
}