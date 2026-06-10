package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.*;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaRunnerFollower implements LoggingServer{
    class FollowerRunnable implements Runnable{
        ServerSocket serverSocket;
        private Logger logger;
        FollowerRunnable(ServerSocket serverSocket) throws IOException {
            this.serverSocket = serverSocket;
            int tcpPort = peerServer.getAddress().getPort() + 2;
            logger = initializeLogging( FollowerRunnable.class.getName() + "-on-" + peerServer.getServerId() + "-on-tcpPort" + tcpPort +":SUMMARY",false);
        }

        @Override
        public void run() {
            logger.info("Started follower...");
            //now we need to handle the fact that we can complete work, and then our leader is gone down.
            while (!Thread.currentThread().isInterrupted()) {
                Vote leader = peerServer.getCurrentLeader();
                InetSocketAddress leaderAddress = peerServer.getPeerByID(leader.getProposedLeaderID());
                String leaderHost = leaderAddress.getHostName();
                String response = null;
                if(serverSocket.isClosed()){
                    break;
                }
                try(
                Socket clientSocket = serverSocket.accept();
                InputStream inputStream = clientSocket.getInputStream();
                OutputStream outputStream = clientSocket.getOutputStream()
                )
                {
                    logger.info("New connection from " + clientSocket);
                    validate(leaderHost, clientSocket);
                    String requestID = null;
                    try {
                        StringBuilder requestIDBuilder = new StringBuilder();
                        int b;
                        while ((b = inputStream.read()) != -1) {
                            if (b == ':') break;
                            requestIDBuilder.append((char) b);
                        }
                        requestID = requestIDBuilder.toString();
                        if(uncompletedResults.containsKey(requestID)){
                            response = uncompletedResults.get(requestID);
                            uncompletedResults.remove(requestID); //null it out.
                        }
                        else
                            response = runner.compileAndRun(inputStream);

                        logger.info("Response parsed as: " + response);
                    }catch(ReflectiveOperationException | IllegalArgumentException e) {
                        ByteArrayOutputStream tmpStream = new ByteArrayOutputStream();
                        PrintStream ps = new PrintStream(tmpStream);
                        e.printStackTrace(ps);
                        String stackTrace = tmpStream.toString();
                        response = e.getMessage() + "\n" + stackTrace;
                    }
                    if(peerServer.getCurrentLeader() == null || peerServer.failedPeers.contains(peerServer.getCurrentLeader().getProposedLeaderID())){
                        //our leader failed, so lets queue the last result.
                        //and exit the loop and return.
                        if(response != null && requestID != null) uncompletedResults.put(requestID,response);
                    }else {
                        outputStream.write(response.getBytes());
                        outputStream.flush();
                        logger.info("Response sent");
                    }
                } catch (IOException e) {
                    logger.log(Level.WARNING,"Logger IO exception occurred while sending to client" +  e.getMessage(), e);
                }
            }
            logger.info("Finished the follower...");
        }
        private void validate(String leaderHost,Socket clientSocket){
            if(clientSocket == null){
                logger.warning("Socket is null");
                throw new IllegalArgumentException("Socket is null");
            }
            InetAddress clientAddress = clientSocket.getInetAddress();
            String clientHost = clientAddress.getHostName();


            if(!leaderHost.equals(clientHost)){
                throw new IllegalArgumentException("Invalid leader host or port");
            }
        }
    }
    //class that runs n a worker node.
    JavaRunner runner;
    PeerServerImpl peerServer;
    Thread followerThread;
    String runID;
    Logger logger;
    FollowerRunnable followerRunnable;
    Map<String,String> uncompletedResults;
    ServerSocket serverSocket;
    public JavaRunnerFollower(PeerServerImpl server, Map<String,String> uncompletedResults,ServerSocket serverSocket) {
        this.peerServer = server;
        int tcpPort = server.getAddress().getPort() + 2;
        this.uncompletedResults = uncompletedResults;
        this.serverSocket = serverSocket;
        try {
            logger = initializeLogging( FollowerRunnable.class.getName() + "-on-" + peerServer.getServerId() + "-on-tcpPort" + tcpPort +":SUMMARY",false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            this.runner = new JavaRunner();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    public void run() {
        try {
            this.followerRunnable = new FollowerRunnable(serverSocket);
            followerThread = new Thread(this.followerRunnable);
            followerThread.setDaemon(true);
            logger.info("Starting follower thread...");
            followerThread.start();
            followerThread.join();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to create server socket", e);
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            logger.log(Level.INFO,"Follower thread interrupted",e);
            shutdown();
        }
        logger.info("Finished the follower thread");
    }
    public void shutdown() {
        logger.info("Shutting down follower thread...");
        if (followerThread != null) {
            followerThread.interrupt();
        }

        try {
            if (followerThread != null) {
                followerThread.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(Level.WARNING, "Follower thread interrupted", e);

    }
    }
    public void ceaseWork() {

        if(this.followerThread != null)
            this.followerThread.interrupt();
        try {
            if (followerThread != null) {
                followerThread.join(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(Level.WARNING,"Follower thread interrupted",e);
        }
    }
}
