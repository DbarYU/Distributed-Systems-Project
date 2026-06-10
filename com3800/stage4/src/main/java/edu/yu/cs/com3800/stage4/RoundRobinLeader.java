package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.Util;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RoundRobinLeader {
    private class WorkRequest {
        Message work;
        Socket socket;
        public WorkRequest(Message work, Socket socket) {
            this.work = work;
            this.socket = socket;
        }
    }


    //instance that is used by the leader to assing work to diff nodes.
    //have 2 threads,
    //1 thread that manages the assignment creation, because returning a result has nothing to do assigning work.
    PeerServer peerServer;
    //keep track of the last server, and run a loop.
    List<InetSocketAddress> servers;
    int lastServer = -1;

    LinkedBlockingQueue<WorkRequest> incomingWorkRequests;

    Map<Long, Long> clientToInternal;
    ExecutorService pool;

    Thread tcpHandlerThread;
    long timeTorun = 3000;

    ServerSocket TCPserverSocket;
    long gatewayID;

    class TCPHandler implements Runnable {
        ServerSocket serverSocket;
        private static final Logger logger = Logger.getLogger(TCPHandler.class.getName() + Thread.currentThread().getName());
        //thread that manages all TCP connections,
        //this runs in the backgroud, accpets TCP connections, and then places on a queue for the assign work thread to assign work to the necassary thread!
        TCPHandler( ServerSocket TCPserverSocket) throws IOException {
            this.serverSocket = TCPserverSocket;
            int tcpPort = TCPserverSocket.getLocalPort();

            String fileName = "logs/" + Globals.RUN_ID + "/edu.yu.cs.com3800.stage4.RoundRobinLeader.TCPHandler-on-"
                    + peerServer.getServerId()
                    + "-on-tcpPort" + tcpPort
                    + "-Log.log";

            FileHandler fh = new FileHandler(fileName, true);
            logger.addHandler(fh);
        }

        @Override
        public void run() {
            logger.info("Starting TCP Server on port " + TCPserverSocket.getLocalPort());
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    InetSocketAddress clientAddress = (InetSocketAddress) clientSocket.getRemoteSocketAddress();
                    String senderHost = clientAddress.getHostName();
                    int senderPort = clientAddress.getPort();
                    logger.log(Level.INFO,"TCP Server Accepted on "+senderHost+":"+senderPort);
                    try {
                            InputStream inputStream = clientSocket.getInputStream();
                        byte[] requestBytes = Util.readAllBytesFromNetwork(inputStream);
                        if (requestBytes.length == 0) {
                            continue;
                        }
                        String receiverHost = serverSocket.getInetAddress().getHostName();
                        int receiverPort = serverSocket.getLocalPort();

                        Message workMessage = new Message(
                                Message.MessageType.WORK,
                                requestBytes,
                                senderHost,
                                senderPort,
                                receiverHost,
                                receiverPort
                        );
                        incomingWorkRequests.offer(new WorkRequest(workMessage,clientSocket));
                        String contents = new String(workMessage.getMessageContents());
                        String truncated = contents.length() > 30 ? contents.substring(0, 30) : contents;
                        logger.log(Level.INFO,"Sent Message: " + truncated);
                    } catch (IOException e) {
                        logger.log(Level.WARNING,"Exception occured while processing request", e);
                        if (!Thread.currentThread().isInterrupted()) {
                            break;
                        }
                    }
                } catch (IOException e) {
                    logger.log(Level.WARNING,"Exception occured while processing request", e);
                    break;
                }
            }
        }
    }
    private static final Logger logger = Logger.getLogger(RoundRobinLeader.class.getName() + Thread.currentThread().getName());
    public RoundRobinLeader(PeerServer peerServer,long gatewayID, Map<Long, InetSocketAddress> peerIDtoAddress) {
        int tcpPort = peerServer.getUdpPort() + 2;
        String fileName = "logs/" + Globals.RUN_ID + "/edu.yu.cs.com3800.stage4.RoundRobinLeader.-on-"
                + peerServer.getServerId()
                + "-on-tcpPort" + tcpPort
                + "-Log.log";

        FileHandler fh;
        try {
            fh = new FileHandler(fileName, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.addHandler(fh);
        this.peerServer = peerServer;
        try {
            this.TCPserverSocket = new ServerSocket(peerServer.getUdpPort() + 2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        servers = new ArrayList<>();
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            InetSocketAddress address = entry.getValue();
            if (address.getHostName().equals(this.peerServer.getAddress().getHostName()) && address.getPort() == this.peerServer.getAddress().getPort())
                continue;
            if (entry.getKey().equals(this.gatewayID))
                continue;
            servers.add(address);

            this.gatewayID = gatewayID;
            this.clientToInternal = new HashMap<>();
            this.incomingWorkRequests = new LinkedBlockingQueue<>();

            try {
                this.tcpHandlerThread = new Thread(new TCPHandler(TCPserverSocket));
                this.tcpHandlerThread.setDaemon(true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }

    public void run() {
        logger.info("Starting TCP Thread..");
        tcpHandlerThread.start();
        pool = Executors.newCachedThreadPool();
        while (!Thread.currentThread().isInterrupted()) {
            try {

                WorkRequest workRequest = this.incomingWorkRequests.take();

                logger.log(Level.INFO,"TCP Request Received, sendng to pool");
                pool.submit(() -> {
                    try {
                        if (workRequest.work.getMessageType() != Message.MessageType.WORK) {
                            workRequest.socket.close();
                        }else {
                            String result = manageWork(workRequest.work); // i need to send this to the designated worker, and await response
                            logger.log(Level.INFO,"TCP Work Response: " + result + "Sending response to Gateway");
                            sendResponseToGateWay(result, workRequest.socket);
                            workRequest.socket.close();
                        }
                    } catch (ServerExceptions.ServerException  | IOException  e) {
                        logger.log(Level.WARNING,"Exception occured while processing request", e);
                        //log the error
                    }

                });
            }
            catch (InterruptedException e) {
                logger.log(Level.INFO,"Interrupted while processing request",e);
                Thread.currentThread().interrupt();
            }
            //work until interrupted...
        }
        logger.info("Work completed");
        shutdown();
    }
    private void sendResponseToGateWay(String result,Socket socket) throws ServerExceptions.ServerException {
        try {

            OutputStream out = socket.getOutputStream();
            out.write(result.getBytes());
            out.flush();
            socket.shutdownOutput();

        } catch (IOException e) {
            throw new ServerExceptions.ConnectionException("Failed to send response to GateWay!", e);
        } catch (Exception e){
            throw new ServerExceptions.ServerException(500, "Failed to send response to GateWay!", e);
        }
    }

    private String manageWork(Message msg) throws ServerExceptions.ServerException {
        int index = lastServer++;
        index = index % servers.size();
        if (index < 0)
            index = 0; //acount for integer oveflow
        InetSocketAddress address = servers.get(index);
        //I have the address, i need to now establish connection with the worker, and wait until it completes.
        //this method manages the requedt between the leader and worker,
        //establish connection with worker, and block until it has completed
        try(Socket socket = new Socket(address.getHostName(), address.getPort() + 2)){
            OutputStream out = socket.getOutputStream();
            out.write(msg.getMessageContents());
            out.flush();
            socket.shutdownOutput();

            InputStream in = socket.getInputStream();
            byte[] buffer =Util.readAllBytesFromNetwork(in);
            return new String(buffer);
        } catch (IOException e) {
            throw new ServerExceptions.ConnectionException("Failed to connect with worker", e);
        }catch (Exception e){
            throw new ServerExceptions.ServerException(500, "Failed to connect with worker", e);
        }

    }
    public void shutdown() {
        shutdownTCPThread();

        if (pool != null) {
            shutDownPool();
        }
    }
    private void shutDownPool() {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void shutdownTCPThread() {

        if (this.tcpHandlerThread != null) {
            this.tcpHandlerThread.interrupt();
        }

        try {
            this.TCPserverSocket.close();
        } catch (IOException e) {
            logger.log(Level.WARNING,"Exception occurred while closing TCP server socket", e);
        }

        try {
            if (this.tcpHandlerThread  != null) {
                this.tcpHandlerThread.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

    }
}
