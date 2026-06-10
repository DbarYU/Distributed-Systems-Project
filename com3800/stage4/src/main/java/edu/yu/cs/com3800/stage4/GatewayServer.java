package edu.yu.cs.com3800.stage4;

import com.sun.net.httpserver.HttpServer;

import com.sun.net.httpserver.*;
import edu.yu.cs.com3800.Util;


import java.io.*;
import java.net.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;


public class GatewayServer {


    private class GatewayHTTPHandler implements HttpHandler {
        private static final Logger logger = Logger.getLogger(GatewayHTTPHandler.class.getName() + Thread.currentThread().getName());
        public GatewayHTTPHandler(int httpPort) {
            String fileName = "logs/" + Globals.RUN_ID + "/edu.yu.cs.com3800.stage4.GatewayServer.GatewayHTTPHandler-on-"
                    + httpPort
                    + "-Log.log";
            FileHandler fh;
            try {
                fh = new FileHandler(fileName, true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            logger.addHandler(fh);
        }
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                logger.log(Level.INFO, "HTTP GET " + exchange.getRequestMethod() + " " + exchange.getRequestURI());
                String requestBody;
                requestBody = parseRequest(exchange); //parse the request,

                int hashCode = requestBody.hashCode(); //check for cache.
                String resp;
                boolean cached = false;
                if(reqToRespCache.containsKey(hashCode)){
                    logger.log(Level.INFO, exchange.getRequestURI() + "CACHED");
                    resp = reqToRespCache.get(hashCode);
                    cached = true;
                }else {
                    logger.log(Level.INFO, exchange.getRequestURI() + "NOT CACHED");

                    resp = sendTCPToLeaderAndAwaitResponse(requestBody);
                    reqToRespCache.put(hashCode, resp);
                }
                logger.log(Level.INFO, "RESPONSE: " + resp);

                sendResponse(exchange,resp,cached);
            }
            catch (ServerExceptions.ServerException e) {
                logger.log(Level.INFO, "ERROR RESP: " + e);
                sendErrorResponse(exchange, e);
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
        private void sendErrorResponse(HttpExchange exchange, ServerExceptions.ServerException e) throws IOException {
            byte[] bytes = e.getMessage().getBytes();
            exchange.sendResponseHeaders(e.getStatusCode(), bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
        private String sendTCPToLeaderAndAwaitResponse(String str) throws ServerExceptions.ServerException   {
            logger.info("Sending to the Leader with TCP and awaiting response...");
            if(gatewayPeerServer.getCurrentLeader() == null){
                logger.log(Level.WARNING, "The leader hasn't been set yet");
                throw new ServerExceptions.InternalServerException("The leader hasn't been set yet");
            }
            if(!peerIDtoAddress.containsKey(gatewayPeerServer.getCurrentLeader().getProposedLeaderID())){
                logger.log(Level.WARNING, "The leader hasn't been set yet");
                throw new ServerExceptions.InternalServerException("Error in setup");
            }

            InetSocketAddress leaderAddress = peerIDtoAddress.get(gatewayPeerServer.getCurrentLeader().getProposedLeaderID());
            String host = leaderAddress.getHostName();

            return getResponseFromTCP(str, leaderAddress, host);
        }

        private static String getResponseFromTCP(String str, InetSocketAddress leaderAddress, String host) throws ServerExceptions.ExecutionException {
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

                socket.close();
            } catch (IOException e) {
                logger.log(Level.WARNING, "Error sending response");
                throw new ServerExceptions.ExecutionException("Error sending response");
            }
            return resp;
        }
    }

    HttpServer httpServer;
    HttpHandler httpHandler;
    GatewayPeerServerImpl gatewayPeerServer;
    ConcurrentHashMap<Integer,String> reqToRespCache;
    ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    AtomicBoolean httpStarted = new AtomicBoolean(false);
    AtomicBoolean peerServerStarted = new AtomicBoolean(false);
    private static final Logger logger = Logger.getLogger(GatewayPeerServerImpl.class.getName() + Thread.currentThread().getName());
    public GatewayServer(int httpPort, int peerPort, long peerEpoch, Long serverID,
                          ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers) throws IOException{


        reqToRespCache = new ConcurrentHashMap<>();
        this.peerIDtoAddress = peerIDtoAddress;
        httpHandler = new GatewayHTTPHandler(httpPort); //our handler that manages the http requests
        httpServer = HttpServer.create(new InetSocketAddress(httpPort),0, "/compileandrun",httpHandler); //initiating http server
        this.gatewayPeerServer = new GatewayPeerServerImpl(peerPort,peerEpoch,serverID,peerIDtoAddress,serverID,numberOfObservers);
        this.gatewayPeerServer.gateAwayServer = this;
        int tcpPort = this.gatewayPeerServer.getUdpPort() + 2;
        String fileName = "logs/" + Globals.RUN_ID + "/edu.yu.cs.com3800.stage4.GatewayServer-on-"
                + this.gatewayPeerServer.getServerId()
                + "-on-tcpPort" + tcpPort
                + "-Log.log";
        FileHandler fh = new FileHandler(fileName, true);
        logger.addHandler(fh);


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
