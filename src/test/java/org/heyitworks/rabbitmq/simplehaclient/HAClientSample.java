package org.heyitworks.rabbitmq.simplehaclient;

import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * User: maciekr
 */
public class HAClientSample {

    public static void main(String[] args) {

        HAClient haClient = new HAClient(new Address[] {new Address("localhost", 6671), new Address("localhost", 7671)},
                "test", "test", "somePass", true);

        boolean isCallback = true;
        haClient.doInRabbit(new HAClient.RabbitOperation() {
            @Override
            public void execute(Connection connection) throws IOException {
                Channel channel = connection.createChannel();
                channel.basicConsume("mr-testq", true, "client1Tag",
                        new DefaultConsumer(channel) {
                            @Override
                            public void handleDelivery(String consumerTag,
                                                       Envelope envelope,
                                                       AMQP.BasicProperties properties,
                                                       byte[] body)
                                    throws IOException {
                                String routingKey = envelope.getRoutingKey();
                                System.out.println("consuming " + new String(body) + "\n with routingKey = " + routingKey);
                            }
                        });
            }

            @Override
            public String operationId() {
                return "CONSUMER-MR-TESTQ";
            }

        }, isCallback);

        haClient.doInRabbit(new HAClient.RabbitOperation() {
            @Override
            public void execute(Connection connection) throws IOException {
                Channel channel = connection.createChannel();
                channel.basicPublish("", "mr-testq", null, "HAClient test message".getBytes());
                System.out.println("Message published");
                channel.close();
            }

            @Override
            public String operationId() {
                return "PUBLISHER-MR-TESTQ";
            }
        });


        try {
            Thread.sleep(100000);
            haClient.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
