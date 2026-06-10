package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpServer;

import com.sun.net.httpserver.*;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.Vote;


import java.io.*;
import java.net.*;

import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;


public class GatewayServer extends Thread  implements Runnable, LoggingServer {
    private class QueuedMessage {
        String requestID;
        String requestContent;
        QueuedMessage(String requestID,String requestContent){
            this.requestID = requestID;
            this.requestContent = requestContent;
        }
    }

    private class GatewayHelperThread extends Thread {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                //if the leader is not set aka leader either died or we havent started yet, dont manage.
                HttpExchange exchange;
                try {
                    exchange = incomingRequests.take();
                    while (true) {
                        if (gatewayPeerServer.getCurrentLeader() == null || gatewayPeerServer.failedPeers.contains(gatewayPeerServer.getCurrentLeader().getProposedLeaderID())) {
                            //we are either in an election, or the leader failed. we need to queue this.
                            Thread.sleep(1000);
                        } else
                            break;
                    }
                } catch (InterruptedException e) {
                    break;
                }
                try {
                    logger.log(Level.INFO, "HTTP GET " + exchange.getRequestMethod() + " " + exchange.getRequestURI());
                    String requestBody;
                    requestBody = parseRequest(exchange); //parse the request,
                    int hashCode = requestBody.hashCode(); //check for cache.
                    String resp;
                    boolean cached = false;
                    if (reqToRespCache.containsKey(hashCode)) {
                        logger.log(Level.INFO, exchange.getRequestURI() + "CACHED");
                        resp = reqToRespCache.get(hashCode);
                        cached = true;
                    } else {
                        logger.log(Level.INFO, exchange.getRequestURI() + "NOT CACHED");
                        resp = sendTCPToLeaderAndAwaitResponse(requestBody);
                        reqToRespCache.put(hashCode, resp);
                    }
                    logger.log(Level.INFO, "RESPONSE: " + resp);
                    //now check again if leader is dead. if it is, we restart the process
                    if (gatewayPeerServer.getCurrentLeader() == null || gatewayPeerServer.failedPeers.contains(gatewayPeerServer.getCurrentLeader().getProposedLeaderID())) {
                        incomingRequests.putFirst(exchange);
                        continue; // put it back first in queue because we cant trust.
                    }
                    sendResponse(exchange, resp, cached);
                } catch (ServerExceptions.ServerException e) {
                    logger.log(Level.INFO, "ERROR RESP: " + e);
                    sendErrorResponse(exchange, e);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }


    private class GatewayHTTPHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) {
            incomingRequests.add(exchange);
        }
    }
    private class GatewayINFOHTTPHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Vote currentLeader = gatewayPeerServer.getCurrentLeader();
            if (currentLeader == null) {
                String response = "NO LEADER SET";
                byte[] bytes = response.getBytes();
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            } else {
                long leaderID = currentLeader.getProposedLeaderID();
                StringBuilder response = new StringBuilder();
                response.append(gatewayPeerServer.getServerId()).append(":observer\n");
                for (Long peerID : peerIDtoAddress.keySet()) {
                    if(gatewayPeerServer.failedPeers.contains(peerID))
                        continue;
                    String role = (peerID.equals(leaderID)) ? "leader" : "follower";
                    response.append(peerID).append(":").append(role).append("\n");
                }
                String responseStr = response.toString();
                byte[] bytes = responseStr.getBytes();
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
        }
    }
    private String parseRequest(HttpExchange exchange) throws ServerExceptions.ServerException {
        try {
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            if(!exchange.getRequestMethod().equals("POST")){
                throw new IllegalArgumentException("Method not supported");
            }else if(contentType == null || !contentType.equals("text/x-java-source")){
                throw new IllegalArgumentException("Content-Type not supported");
            }else {
                //valid request, lets process the data and leverage javaRunner so we can translate the string into executable code.
                return new String(exchange.getRequestBody().readAllBytes()).trim().strip();
            }
        }catch (IOException e){
            throw new ServerExceptions.InternalServerException("Error parsing request");
        }

    }
    private void sendResponse(HttpExchange exchange,String response, boolean cached) throws ServerExceptions.ServerException {
        byte[] bytes = response.getBytes();
        try {
            exchange.getResponseHeaders().add("Cached-Response", cached ? "true" : "false");
            exchange.sendResponseHeaders(200, bytes.length);
        } catch (IOException e) {
            throw new ServerExceptions.ExecutionException("Error sending response headers");
        }

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        } catch (IOException e) {
            throw new ServerExceptions.ExecutionException("Error sending response");
        }

    }
    private void sendErrorResponse(HttpExchange exchange, ServerExceptions.ServerException e) {
        try {
            byte[] bytes = e.getMessage().getBytes();
            exchange.sendResponseHeaders(e.getStatusCode(), bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }
    private String sendTCPToLeaderAndAwaitResponse(String str) throws ServerExceptions.ServerException {
        logger.info("Sending to the Leader with TCP and awaiting response...");

        int maxRetries = 50; //some upper bound so eventually it will be done
        int retryCount = 0;

        while (!Thread.currentThread().isInterrupted() && retryCount < maxRetries) {
            Vote currentLeader = gatewayPeerServer.getCurrentLeader();

            while (currentLeader == null || gatewayPeerServer.failedPeers.contains(currentLeader.getProposedLeaderID())) {
                try {
                    logger.log(Level.INFO, "No valid leader, waiting...");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ServerExceptions.InternalServerException("Interrupted while waiting for leader");
                }
                currentLeader = gatewayPeerServer.getCurrentLeader();
            }
            if (!peerIDtoAddress.containsKey(currentLeader.getProposedLeaderID())) {
                logger.log(Level.WARNING, "Leader not in address map");
                throw new ServerExceptions.InternalServerException("Error in setup");
            }

            InetSocketAddress leaderAddress = peerIDtoAddress.get(currentLeader.getProposedLeaderID());
            String host = leaderAddress.getHostName();

            try {
                return getResponseFromTCP(str, leaderAddress, host);
            } catch (ServerExceptions.ExecutionException e) {
                retryCount++;
                if (gatewayPeerServer.failedPeers.contains(currentLeader.getProposedLeaderID())) {
                    logger.log(Level.INFO, "Leader " + currentLeader.getProposedLeaderID() + " is confirmed dead, will get new leader");
                } else {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new ServerExceptions.InternalServerException("Interrupted during retry");
                    }
                }
            }
        }

        throw new ServerExceptions.ServiceUnavailableException("Failed to send request after " + maxRetries + " attempts");
    }


    private String getResponseFromTCP(String str, InetSocketAddress leaderAddress, String host) throws ServerExceptions.ExecutionException {
            int port = leaderAddress.getPort() + 2;
            String resp;

            try {
                Socket socket = new Socket(host, port);
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                out.write(str.getBytes());
                out.flush();
                socket.shutdownOutput();

                byte[] buffer = Util.readAllBytesFromNetwork(in);
                resp = new String(buffer);

            } catch (IOException e) {
                logger.log(Level.WARNING, "Error sending response" + e.getMessage());
                throw new ServerExceptions.ExecutionException("Error sending response");
                //retry
            }
            return resp;
        }


    HttpServer httpServer;
    HttpHandler httpHandler;
    GatewayPeerServerImpl gatewayPeerServer;
    ConcurrentHashMap<Integer,String> reqToRespCache;
    ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    AtomicBoolean httpStarted = new AtomicBoolean(false);
    AtomicBoolean peerServerStarted = new AtomicBoolean(false);
    LinkedBlockingDeque<HttpExchange> incomingRequests = new LinkedBlockingDeque<>();
    GatewayHelperThread gatewayHelperThread;
    Queue<QueuedMessage> uncompletedRequests;
    private static Logger logger;


    public GatewayServer(int httpPort, int peerPort, long peerEpoch, Long serverID,
                          ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers) throws IOException{

        reqToRespCache = new ConcurrentHashMap<>();
        this.peerIDtoAddress = peerIDtoAddress;
        this.httpHandler = new GatewayHTTPHandler();

        this.httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
        httpServer.createContext("/compileandrun",httpHandler);
        httpServer.createContext( "/info",new GatewayINFOHTTPHandler());
        //initiating http server

        this.gatewayPeerServer = new GatewayPeerServerImpl(peerPort,peerEpoch,serverID,peerIDtoAddress,serverID,numberOfObservers);
        this.gatewayPeerServer.gateAwayServer = this;
        int tcpPort = this.gatewayPeerServer.getUdpPort() + 2;
        logger = initializeLogging(
                this.getName() + "-on-" + this.gatewayPeerServer.id + "-on-tcpPort" + tcpPort +":REGULAR", true
        );

        this.uncompletedRequests = new LinkedBlockingDeque<>();
        this.gatewayHelperThread = new GatewayHelperThread();


    }
    @Override
    public void run(){
        this.gatewayHelperThread.start();
        startPeerServer();
        startHttpServer();

        try {
            synchronized (this) {
                this.wait();
            }
        }catch(InterruptedException e){
            Thread.currentThread().interrupt();
        }
        this.shutdown();
    }

    public GatewayPeerServerImpl getPeerServer(){
        return gatewayPeerServer;
    }
    public void shutdown() {
        logger.info("Shutting down...");
        this.httpServer.stop(0);
        this.gatewayPeerServer.shutdown();
        try {
            this.gatewayPeerServer.join();
        }  catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void startPeerServer(){
        if(!this.peerServerStarted.getAndSet(true))
            this.gatewayPeerServer.start();
    }
    public void startHttpServer(){
        if(!this.httpStarted.getAndSet(true))
            this.httpServer.start(); //start the http server that sits on /compileandrun

    }
}
