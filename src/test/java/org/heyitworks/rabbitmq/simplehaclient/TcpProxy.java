package org.heyitworks.rabbitmq.simplehaclient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class TcpProxy {

    private TcpProxy() {
    }

    private static ServerSocket serverSocket;
    private static AtomicBoolean up = new AtomicBoolean();

    private static ExecutorService executorService = Executors.newSingleThreadExecutor();

    private static Socket client = null;
    private static Socket server = null;

    /**
     * super simple single-threaded proxy server for testing
     */
    public static synchronized void runProxy(final String remoteHost, final int remotePort, final int localPort)
            throws IOException {

        serverSocket = new ServerSocket(localPort);
        up.set(true);
        final byte[] request = new byte[1024];
        final byte[] reply = new byte[4096];

        executorService.submit(new Runnable() {
            @Override
            public void run() {
                while (up.get()) {
                    client = null;
                    server = null;
                    try {
                        client = serverSocket.accept();
                        final InputStream streamFromClient = client.getInputStream();
                        final OutputStream streamToClient = client.getOutputStream();
                        try {
                            server = new Socket(remoteHost, remotePort);
                        } catch (IOException e) {
                            if (client != null)
                                client.close();
                            continue;
                        }

                        final InputStream streamFromServer = server.getInputStream();
                        final OutputStream streamToServer = server.getOutputStream();

                        Thread t = new Thread() {
                            public void run() {
                                int bytesRead;
                                try {
                                    while ((bytesRead = streamFromClient.read(request)) != -1) {
                                        streamToServer.write(request, 0, bytesRead);
                                        streamToServer.flush();
                                    }
                                } catch (IOException e) {
                                }
                                try {
                                    streamToServer.close();
                                } catch (IOException e) {
                                }
                            }
                        };

                        t.start();

                        int bytesRead;
                        try {
                            while ((bytesRead = streamFromServer.read(reply)) != -1) {
                                streamToClient.write(reply, 0, bytesRead);
                                streamToClient.flush();
                            }
                        } catch (IOException e) {
                        }
                        streamToClient.close();

                    } catch (IOException e) {
                        System.err.println(e);
                    } finally {
                        try {
                            if (server != null)
                                server.close();
                            if (client != null)
                                client.close();
                        } catch (IOException e) {
                        }
                    }
                }
            }
        });
    }

    public static synchronized void killClientConnection() throws IOException {
        if (client != null)
            client.close();
    }

    public static synchronized void shutdownProxy() throws IOException {
        up.set(false);
        if (serverSocket != null)
            serverSocket.close();
    }
}
