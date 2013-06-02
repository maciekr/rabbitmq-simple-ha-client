package org.heyitworks.rabbitmq.simplehaclient;

import com.rabbitmq.client.*;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

public class RabbitHAClientTest {

    private static String targetHost = System.getProperty("haRabbitClient.test.targetHost", "38.61.197.191");
    private static String vhost = System.getProperty("haRabbitClient.test.vhost", "unittest");
    private static String user = System.getProperty("haRabbitClient.test.user", "unittester");
    private static String pass = System.getProperty("haRabbitClient.test.pass", "TS03dev08");

    @BeforeClass
    public static void globalSetup() {

    }

    @Test
    public void happyPath_NoSSL_WithReconnection() throws Exception {

        final int MSG_COUNT = 10;
        final Set<String> msgs = Collections.newSetFromMap(new ConcurrentHashMap());

        TcpProxy.runProxy(targetHost, 5672, 5672);

        RabbitHAClient haClient = new RabbitHAClient(new Address[]{new Address("localhost", 5672)},
                vhost, user, pass, false);

        boolean isCallback = true;
        haClient.doInRabbit(new RabbitHAClient.RabbitOperation() {
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
                                String msg = new String(body);
                                System.out.println(Thread.currentThread() + " consuming " + msg + "\n with routingKey = " + routingKey);
                                msgs.remove(msg);
                            }
                        });
            }

            @Override
            public String operationId() {
                return "CONSUMER-MR-TESTQ";
            }

        }, isCallback);

        final Random r = new Random();

        Thread t = new Thread("Random-Conn-Killer") {
            @Override
            public void run() {
                try {
                    while (true) {
                        TcpProxy.killClientConnection();
                        Thread.sleep(r.nextInt(5000));
                    }
                } catch (IOException e) {
                } catch (InterruptedException e) {
                }
            }
        };
        t.setDaemon(true);
        t.start();


        for (int i = 0; i < MSG_COUNT; i++) {
            final String uuid = UUID.randomUUID().toString();
            msgs.add(uuid);
            haClient.doInRabbit(new RabbitHAClient.RabbitOperation() {
                @Override
                public void execute(Connection connection) throws IOException {
                    Channel channel = connection.createChannel();
                    channel.basicPublish("", "mr-testq", null, uuid.getBytes());
                    System.out.println("Message published");
                    if (channel.getConnection().isOpen())
                        channel.close();
                }

                @Override
                public String operationId() {
                    return "PUBLISHER-MR-TESTQ";
                }
            });
            try {
                Thread.sleep(r.nextInt(1000));
            } catch (InterruptedException e) {
            }
        }

        Thread.sleep(30000);
        haClient.shutdown();
        TcpProxy.shutdownProxy();
        assertEquals(0, msgs.size());
    }
}
