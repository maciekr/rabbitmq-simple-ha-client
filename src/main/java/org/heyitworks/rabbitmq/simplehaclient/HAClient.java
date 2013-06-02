package org.heyitworks.rabbitmq.simplehaclient;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * a simple implementation of a high availability client for RabbitMq
 *
 * @author : maciekr
 */
public class HAClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(HAClient.class);

    private static final int MAX_RETRIES = 5;
    private static final int RETRY_DELAY = 5000;

    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Address[] addresses;

    private Set<RabbitOperation> callbacks = Collections.newSetFromMap(new ConcurrentHashMap<RabbitOperation, Boolean>());

    public HAClient(Address[] addresses, String vhost, String user, String pass, boolean useSSL) {
        this.addresses = addresses;
        this.connectionFactory = new ConnectionFactory();
        try {
            connectionFactory.setVirtualHost(vhost);
            connectionFactory.setUsername(user);
            connectionFactory.setPassword(pass);
            if (useSSL)
                connectionFactory.useSslProtocol();
        } catch (Exception e) {
            throw new RuntimeException("Rabbit ConnectionFactory construction failed!", e);
        }
    }

    public interface RabbitOperation {
        void execute(Connection connection) throws IOException;
        String operationId();
    }

    public void doInRabbit(RabbitOperation operation) {
        new DoInRabbit().doInRabbit(operation);
    }

    public void doInRabbit(RabbitOperation operation, boolean isCallback) {
        new DoInRabbit().doInRabbit(operation, isCallback);
    }

    public void doInRabbit(RabbitOperation operation, boolean isCallback, boolean failFast) {
        new DoInRabbit().doInRabbit(operation, isCallback, failFast);
    }

    public void shutdown() throws IOException {
        if (connection != null && connection.isOpen()) {
            LOGGER.info("Shutting down HA client and its connection {}.", connection);
            connection.close();
            callbacks = null;
        }
    }

    private synchronized Connection getConnection() throws IOException {

        if (connection == null || !connection.isOpen()) {
            LOGGER.info("Attempting to connect to {}", Arrays.toString(addresses));
            connection = connectionFactory.newConnection(addresses);
            LOGGER.info("Connected to {}", connection);
            connection.addShutdownListener(new ShutdownListener() {
                @Override
                public void shutdownCompleted(ShutdownSignalException e) {
                    if (!e.isInitiatedByApplication()) {
                        LOGGER.warn("Remote shutdown: {}. Will attempt to recover the connection on remote shutdown.", e.getReason());
                        if (retryConnection(false)) {
                            for (RabbitOperation operation : callbacks) {
                                LOGGER.warn("Recovering registered callback RabbitOperation {}", operation);
                                doInRabbit(operation, false); //just peek and keep in the set
                            }
                        }
                    }
                }
            });
        }
        return connection;
    }

    private synchronized boolean recoverConnection(int retry) {
        try {
            LOGGER.warn("Delaying attempt to recover RabbitMQ connection {}", connection);
            Thread.sleep(RETRY_DELAY);
            LOGGER.warn("Attempting to recover RabbitMQ connection {}", connection);
            if (connection != null)
                try {
                    connection.close();
                    connection = null;
                } catch (Exception e) {}

            connection = getConnection();
            return true;
        } catch (IOException e) {
            if (retry == MAX_RETRIES)
                throw new RuntimeException("Can't recover RabbitMQ connection", e);
            else
                LOGGER.error("Recovery attempt number {} failed with {}.", retry, e.getMessage());
        } catch (InterruptedException e) {
        }
        return false;
    }

    private synchronized boolean retryConnection(boolean failFast) {
        if (!failFast)
            for (int i = 1; i <= MAX_RETRIES; i++) {
                if (recoverConnection(i)) {
                    return true;
                }
            }
        return false;
    }

    private class DoInRabbit {
        void doInRabbit(RabbitOperation operation, boolean isCallback, boolean failFast) {
            try {
                if (isCallback) {
                    LOGGER.info("Flagging RabbitOperation {} as callback and attaching to shutdown hook recovery.", operation.operationId());
                    callbacks.add(operation); //attach to shutdown hook
                }
                operation.execute(getConnection());
            } catch (IOException e) {
                LOGGER.error("RabbitOperation {} failed with IOException", operation.operationId(), e); ;
                if (retryConnection(failFast)) {
                    doInRabbit(operation, isCallback, failFast);
                }
            }
        }

        void doInRabbit(RabbitOperation operation, boolean isCallback) {
            doInRabbit(operation, isCallback, false);
        }

        void doInRabbit(RabbitOperation operation) {
            doInRabbit(operation, false, false);
        }
    }
}
