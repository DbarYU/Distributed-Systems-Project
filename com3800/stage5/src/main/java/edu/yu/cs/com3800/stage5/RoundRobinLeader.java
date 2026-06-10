package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.Util;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RoundRobinLeader implements LoggingServer {
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
    Map<Long,InetSocketAddress> servers;
    AtomicInteger lastServer =new AtomicInteger(-1);

    LinkedBlockingQueue<WorkRequest> incomingWorkRequests;

    Map<Long, Long> clientToInternal;
    ExecutorService pool;

    Thread tcpHandlerThread;

    ServerSocket TCPserverSocket;
    Set<Long> failedPeers;
    long gatewayID;

    Map<String,String> uncompletedResults;
    class TCPHandler implements Runnable , LoggingServer {
        ServerSocket serverSocket;
        private Logger logger;
        //thread that manages all TCP connections,
        //this runs in the backgroud, accpets TCP connections, and then places on a queue for the assign work thread to assign work to the necassary thread!
        TCPHandler(ServerSocket TCPserverSocket) throws IOException {
            this.serverSocket = TCPserverSocket;
            int tcpPort = TCPserverSocket.getLocalPort();
            String name = this.getClass().getName() + "-on-" + gatewayID + "-on-tcpPort" + tcpPort +":REGULAR";
            logger = initializeLogging(name,true);
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
    private Logger logger;
    public RoundRobinLeader(PeerServer peerServer,long gatewayID, Map<Long, InetSocketAddress> peerIDtoAddress,Set<Long> failedPeers,Map<String,String> uncompletedResults,ServerSocket TCPserverSocket) {
        this.peerServer = peerServer;
        int tcpPort = peerServer.getUdpPort() + 2;
        try {
            logger = initializeLogging(RoundRobinLeader.class.getName() + "-on-" + this.gatewayID + "-on-tcpPort" + tcpPort +":REGULAR");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        initializeServers(peerIDtoAddress);
        this.gatewayID = gatewayID;
        this.clientToInternal = new HashMap<>();
        this.incomingWorkRequests = new LinkedBlockingQueue<>();
        this.failedPeers = failedPeers;
        this.uncompletedResults = uncompletedResults;
        this.TCPserverSocket = TCPserverSocket;

    }
    private void initializeServers(Map<Long, InetSocketAddress> peerIDtoAddress){
        servers = new TreeMap<>();
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            InetSocketAddress address = entry.getValue();
            if (address.getHostName().equals(this.peerServer.getAddress().getHostName()) && address.getPort() == this.peerServer.getAddress().getPort())
                continue;
            if (entry.getKey().equals(this.gatewayID))
                continue;
            servers.put(entry.getKey(),address);
        }
    }

    public void run() {
        logger.info("Starting TCP Thread..");
        try {
                this.tcpHandlerThread = new Thread(new TCPHandler(TCPserverSocket));
                this.tcpHandlerThread.setDaemon(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.tcpHandlerThread.setDaemon(true);
        tcpHandlerThread.start();

        pool = Executors.newCachedThreadPool();
        while (!Thread.currentThread().isInterrupted()) {
            try {

                WorkRequest workRequest = this.incomingWorkRequests.take();

                logger.log(Level.INFO,"TCP Request Received, sendng to pool");
                pool.submit(() -> {
                    try {
                        if (workRequest.work.getMessageType() == Message.MessageType.WORK) {
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
       List<Map.Entry<Long,InetSocketAddress>> serverList =  servers.entrySet().stream().toList();
       if(failedPeers.size() >= this.servers.size())
           throw new ServerExceptions.ServiceUnavailableException("ALl peers are unavailable");
       InetSocketAddress address = null;
       int count = 0;
        while(count < serverList.size()) {
            int index = lastServer.getAndIncrement();
            index = index % servers.size();
            if (index < 0)
                index = 0; //acount for integer oveflow
            Map.Entry<Long, InetSocketAddress> entry = serverList.get(index);
            if (!failedPeers.contains(entry.getKey())){
                address = entry.getValue();
                break;
            }
            count++;
        }
        if(address == null)
            throw new ServerExceptions.ServiceUnavailableException("ALl peers are unavailable");
        if(this.uncompletedResults.containsKey(String.valueOf(msg.getRequestID()))){
            //this was completed by the node when it was a worker
            String res = uncompletedResults.get(String.valueOf(msg.getRequestID()));
            uncompletedResults.remove(String.valueOf(msg.getRequestID()));
            return res;
        }
        //I have the address, i need to now establish connection with the worker, and wait until it completes.
        //this method manages the requedt between the leader and worker,
        //establish connection with worker, and block until it has completed
        try(Socket socket = new Socket(address.getHostName(), address.getPort() + 2)){
            OutputStream out = socket.getOutputStream();
            String request = msg.getRequestID() + ":" + new String(msg.getMessageContents());

            out.write(request.getBytes()); //include the request ID so we can map it to  new requests.
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
    public void ceaseWork(){
        if (this.tcpHandlerThread != null) {
            this.tcpHandlerThread.interrupt();
            try {
                this.tcpHandlerThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (this.pool != null) {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
            this.pool = null;
        }
        this.tcpHandlerThread = null;
    }

    private void shutdownTCPThread() {
        if (this.tcpHandlerThread != null) {
            this.tcpHandlerThread.interrupt();
        }
        try {
            if (this.tcpHandlerThread != null) {
                this.tcpHandlerThread.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
