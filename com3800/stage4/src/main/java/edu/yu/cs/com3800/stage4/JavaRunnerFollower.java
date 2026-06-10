package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaRunnerFollower {
    class FollowerRunnable implements Runnable{
        ServerSocket serverSocket;
        private static final Logger logger = Logger.getLogger(FollowerRunnable.class.getName() + Thread.currentThread().getName());
        FollowerRunnable(ServerSocket serverSocket) throws IOException {
            this.serverSocket = serverSocket;
            int tcpPort = peerServer.getAddress().getPort() + 2;

            String fileName = "logs/" + Globals.RUN_ID + "/edu.yu.cs.com3800.stage4.JavaRunnerFollower.FollowerRunnable-on-"
                    + peerServer.getServerId()
                    + "-on-tcpPort" + tcpPort
                    + "-Log.log";
            FileHandler fh = new FileHandler(fileName, true);
            logger.addHandler(fh);
        }

        @Override
        public void run() {
            logger.info("Started follower...");
            while (!Thread.currentThread().isInterrupted()) {
                Vote leader = peerServer.getCurrentLeader();
                InetSocketAddress leaderAddress = peerServer.getPeerByID(leader.getProposedLeaderID());
                String leaderHost = leaderAddress.getHostName();
                String response;
                try(
                Socket clientSocket = serverSocket.accept();
                InputStream inputStream = clientSocket.getInputStream();
                OutputStream outputStream = clientSocket.getOutputStream()
                ) {
                    logger.info("New connection from " + clientSocket);
                    validate(leaderHost, clientSocket);
                    try {
                        response = runner.compileAndRun(inputStream);
                        logger.info("Response parsed as: " + response);
                    }catch(ReflectiveOperationException | IllegalArgumentException e){
                        ByteArrayOutputStream tmpStream = new ByteArrayOutputStream();
                        String stackTrace = tmpStream.toString();
                        response = e.getMessage() +"\\n" + stackTrace;
                        logger.info("Response parsed as: " + response);

                    }
                    outputStream.write(response.getBytes());
                    outputStream.flush();
                    logger.info("Response sent");
                } catch (IOException e) {
                    logger.log(Level.WARNING,"Logger IO exception occurred while sending to client" +  e.getMessage(), e);
                }
            }
            try {
                if (serverSocket != null && !serverSocket.isClosed()) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                logger.log(Level.WARNING,"Logger IO exception occurred while closing serverSocket",e);
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
    ServerSocket serverSocket;

    PeerServer peerServer;
    long timeToRun = 3000;
    Thread followerThread;
    String runID;
    Logger logger = Logger.getLogger(JavaRunnerFollower.class.getName());
    public JavaRunnerFollower(PeerServer server) {
        int tcpPort = server.getAddress().getPort() + 2;
        runID = Globals.RUN_ID;
        String fileName = "logs/" + Globals.RUN_ID + "/edu.yu.cs.com3800.stage4.JavaRunnerFollower-on-"
                + server.getServerId()
                + "-on-tcpPort" + tcpPort
                + "-Log.log";

        FileHandler fh;
        try {
            fh = new FileHandler(fileName, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.addHandler(fh);

        try {
            this.runner = new JavaRunner();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.peerServer = server;
        try {
            serverSocket = new ServerSocket(this.peerServer.getUdpPort() + 2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public void run() {
        try {
            followerThread = new Thread(new FollowerRunnable(serverSocket));
            followerThread.setDaemon(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.info("Starting follower thread...");
        followerThread.start();
        try {
            followerThread.join();
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
            this.serverSocket.close();
        } catch (IOException e) {
            logger.log(Level.WARNING,"Logger IO exception occurred while closing serverSocket",e);
        }

        try {
            if (followerThread != null) {
                followerThread.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(Level.WARNING,"Follower thread interrupted",e);
        }

    }
}
