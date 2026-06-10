package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.Vote;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaRunnerFollower {
    class FollowerRunnable implements Runnable{
        @Override
        public void run() {

            Vote leader = peerServer.getCurrentLeader();
            InetSocketAddress leaderAddress = peerServer.getPeerByID(leader.getProposedLeaderID());
            String leaderHost = leaderAddress.getHostName();
            int leaderPort = leaderAddress.getPort();

            while (!Thread.currentThread().isInterrupted()) {
                logger.info("FOLLOWER EXPECTING - Leader Host: '" + leaderHost + "', Leader Port: " + leaderPort);
                Message msg;
                try {
                    msg = incomingQueue.take();
                }catch (InterruptedException e){
                    break;
                }
                logger.log(Level.INFO, "Got a message from " + msg.getSenderHost() + ":" + msg.getSenderPort() + " Message contents: " + msg);                if (msg.getMessageType().getChar() != 'W' || !msg.getSenderHost().equals(leaderHost) || msg.getSenderPort() != leaderPort) { //we only accept Work based requests, and only requests from our leader.
                incomingQueue.offer(msg); //add it back becasue we want our other threads to handle
                logger.log(Level.INFO,"Message: " +msg  + " Was not relevant!");

                }else {
                    logger.log(Level.INFO,"Parsing Message: " + msg);
                    InputStream messageInput = new ByteArrayInputStream(msg.getMessageContents());
                    InetSocketAddress socketAddress = new InetSocketAddress(leaderHost, leaderPort);
                    String res;
                    try {
                        res = runner.compileAndRun(messageInput);
                    } catch (Exception e ) {
                        res = "Error getting Message: " + e.getMessage();
                    }
                    logger.log(Level.INFO, "Sending Message: " + res);
                    sendMessageToLeader(res, socketAddress,msg.getRequestID());
                }

            }
            logger.info("Interrupted! Shutting down");
        }
    }
    //class that runs n a worker node.
    JavaRunner runner;
    Logger logger;
    LinkedBlockingQueue<Message> incomingQueue;
    LinkedBlockingQueue<Message> outgoingMessages;
    PeerServer peerServer;
    long timeToRun = 3000;
    Thread followerThread;
    public JavaRunnerFollower(PeerServer server, LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> outgoingMessages,Logger logger) {
        try {
            this.runner = new JavaRunner();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.logger = logger;
        this.incomingQueue = incomingMessages;
        this.peerServer = server;
        this.outgoingMessages = outgoingMessages;
    }
    public void run(){
        followerThread = new Thread(new FollowerRunnable());
        logger.log(Level.INFO, "Starting Follower Thread");
        followerThread.start();
        try {
            Thread.sleep(timeToRun);
        }catch (InterruptedException e){
            Thread.currentThread().interrupt();
        }

        followerThread.interrupt();
        logger.log(Level.INFO, "Interrupting Follower Thread");

        try {
            followerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        shutdown();
        logger.log(Level.INFO, "Session completed!");
    }

    private void sendMessageToLeader(String msg,InetSocketAddress address,long requestID){
        Message completedMessage = new Message(Message.MessageType.COMPLETED_WORK,msg.getBytes(),peerServer.getAddress().getHostName(),peerServer.getAddress().getPort(),address.getHostName(),address.getPort(),requestID);
        try {
            this.outgoingMessages.put(completedMessage);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public void shutdown(){
        if(followerThread != null)
            followerThread.interrupt();
        logger.log(Level.INFO, "Interrupting Follower Thread");

        try {
            if(followerThread != null)
                followerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        logger.log(Level.INFO, "Session completed!");
    }
}
