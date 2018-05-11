package io.bittiger.crawler;
import com.rabbitmq.client.*;

import java.io.IOException;

public class Main {
    private static String queue = "q_test";


    public static void main(String[] args) throws Exception{
	// write your code here
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        Connection connection = factory.newConnection();
        Channel inChannel = connection.createChannel();
        inChannel.basicQos(10); // Per consumer limit
        //inChannel.exchangeDeclare("ex_test", "direct", true);


        //direct
        //inChannel.exchangeDeclare("e_fanout_demo2", "fanout", true);
        //String queueName = inChannel.queueDeclare().getQueue();
        //System.out.println(queueName);
        //inChannel.queueBind("q_fanout_demo3", "e_fanout_demo2", "");


        //topic
        //inChannel.exchangeDeclare("e_topic_demo","topic", true);
        inChannel.queueBind("q_warn_log","e_topic_logs","*.warn");


        //inChannel.queueDeclare(queue, true, false, false, null);
        Consumer consumer = new DefaultConsumer(inChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("consumerTag:" + consumerTag);
                System.out.println(" [x] Received '" +envelope.getRoutingKey() + ":" + message + "'");
            }
        };
        inChannel.basicConsume("q_warn_log", true, consumer);
    }
}
