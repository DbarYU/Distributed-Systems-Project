package edu.yu.cs.com3800;

import edu.yu.cs.com3800.stage4.Globals;
import edu.yu.cs.com3800.stage4.JavaRunnerFollower;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UDPMessageSender extends Thread implements LoggingServer {
    private LinkedBlockingQueue<Message> outgoingMessages;
    private int serverUdpPort;
    private static final Logger logger = Logger.getLogger(UDPMessageSender.class.getName() + Thread.currentThread().getName());

    public UDPMessageSender(LinkedBlockingQueue<Message> outgoingMessages, int serverUdpPort) {
        this.outgoingMessages = outgoingMessages;
        setDaemon(true);
        this.serverUdpPort = serverUdpPort;
        setName("UDPMessageSender-port-" + this.serverUdpPort);
        int tcpPort = serverUdpPort + 2;
        String fileName = "logs/" + Globals.RUN_ID + "/edu.yu.cs.com3800.UDPMessageSender"
                + "-on-tcpPort" + tcpPort
                + "-Log.log";
        FileHandler fh;
        try {
            fh = new FileHandler(fileName, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.addHandler(fh);
    }

    public void shutdown() {
        interrupt();
    }

    @Override
    public void run() {
        logger.info("UDPMessageSender started.....");
        while (!this.isInterrupted()) {
            try {
                Message messageToSend = this.outgoingMessages.poll();
                if (messageToSend != null) {
                    logger.info(String.format("UDPMessageSender received %s", messageToSend));
                    DatagramSocket socket = new DatagramSocket();
                    byte[] payload = messageToSend.getNetworkPayload();
                    DatagramPacket sendPacket = new DatagramPacket(payload, payload.length, new InetSocketAddress(messageToSend.getReceiverHost(), messageToSend.getReceiverPort()));
                    socket.send(sendPacket);
                    socket.close();
                }
            }
            catch (IOException e) {
                logger.info("UDPMessageSender Interrupted.....");

            }
        }
    }
}