package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.*;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;



public class PeerServerImpl extends Thread implements PeerServer,LoggingServer {
    class DispatcherThread extends Thread {
        @Override
        public void run() {
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    Message msg = UDPincomingMessages.take();
                    long receiveTime = System.currentTimeMillis();
                    String senderMachine = msg.getSenderHost() + ":" + msg.getSenderPort();
                    String messageContents = new String(msg.getMessageContents());
                    verboseLogger.log(Level.INFO, String.format(
                            "Message received at time [%d] from machine [%s] with contents: %s",
                            receiveTime,
                            senderMachine,
                            messageContents
                    ));

                    switch (msg.getMessageType()) {
                        case GOSSIP -> HEARTBEATincomingMessages.put(msg);
                        case ELECTION -> LEADERELECTIONincomingMessages.put(msg);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    class LeaderMonitorThread extends Thread {
        @Override
        public void run() {
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    synchronized (deadNodeMonitor) {
                        while(currentLeader == null || !failedPeers.contains(currentLeader.getProposedLeaderID()))
                            {
                                deadNodeMonitor.wait();
                            }
                    }
                    changeState(ServerState.LOOKING);
                    //leader is now in the failedPeers set. need to change state, and notify the others.
                    //change the state.
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    private void changeState(ServerState serverState) {
        if(followerClass != null)
            followerClass.ceaseWork();
        if(leaderClass != null)
            leaderClass.ceaseWork();
        System.out.println("[" + this.id +"]:  switching from [" + this.state + "] to [" + serverState + "]");
        if(this.tcpServerSocket != null) {
            try {
                if(!this.tcpServerSocket.isClosed()) {
                    this.tcpServerSocket.close();
                }
                this.tcpServerSocket = new ServerSocket();
                this.tcpServerSocket.setReuseAddress(true);
                this.tcpServerSocket.bind(new InetSocketAddress(this.udpPort + 2));

                if(followerClass != null) {
                    followerClass.serverSocket = this.tcpServerSocket;
                }
                if(leaderClass != null) {
                    leaderClass.TCPserverSocket = this.tcpServerSocket;
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Failed to recreate TCP socket during state change", e);
            }
        }

        this.currentLeader = null;
        setPeerState(serverState);
    }
    protected final InetSocketAddress udpAddress;
    protected final int udpPort;

    protected volatile ServerState state;
    protected volatile AtomicBoolean shutdown;


    protected final Long id;

    protected volatile Vote currentLeader;
    protected final ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress;

    UDPMessageSender udpMessageSender;
    UDPMessageReceiver udpMessageReceiver;
    Thread dispatcherThread;

    protected  LeaderElection leaderElection;
    String myHostName;

    protected final int quorom;
    protected final int numberOfObservers;

    long peerEpoch;
    long gateAwayID;

    JavaRunnerFollower followerClass;
    RoundRobinLeader leaderClass;
    Set<Long> failedPeers;
    Logger logger;
    LinkedBlockingQueue<Message> UDPincomingMessages;
    LinkedBlockingQueue<Message> UDPoutgoingMessages;

    LinkedBlockingQueue<Message> HEARTBEATincomingMessages;
    LinkedBlockingQueue<Message> LEADERELECTIONincomingMessages;

    Thread heartbeatThread;
    final Object deadNodeMonitor;
    Thread leaderMonitorThread;
    Logger verboseLogger;
    Logger summaryLogger;
    ServerSocket tcpServerSocket;
    HttpServer httpServer;
    private String logDirectory;  // e.g., "logs-2026-01-07-11_28"
    private String verboseLogFileName;  // just the prefix
    private String summaryLogFileName;  // just the prefix

    public PeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) throws IOException {
        //code here...
        this.udpPort = udpPort;
        this.numberOfObservers = numberOfObservers;
        this.udpAddress = new InetSocketAddress("localhost",udpPort);
        this.peerIDtoAddress = new ConcurrentHashMap<>(peerIDtoAddress); //addresses of all the other servers in th cluster
        this.id = serverID;
        this.state = ServerState.LOOKING; //currently looking for the current leader.
        this.currentLeader = null; //we dont have a leader set yet, set to null
        this.shutdown = new AtomicBoolean(false);
        this.quorom = (peerIDtoAddress.size() + 1 - numberOfObservers) / 2 + 1;
        this.peerEpoch = peerEpoch;
        this.gateAwayID = gatewayID;
        this.deadNodeMonitor = new Object();

        LocalDateTime date = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-kk_mm");
        String suffix = date.format(formatter);
        this.logDirectory = "logs-" + suffix;

        int tcpPort = this.udpPort + 2;

        this.verboseLogFileName = this.getName() + "-on-" + this.id + "-on-tcpPort" + tcpPort + ":VERBOSE";
        this.summaryLogFileName = this.getName() + "-on-" + this.id + "-on-tcpPort" + tcpPort + ":SUMMARY";
        verboseLogger = initializeLogging(verboseLogFileName, true);
        summaryLogger = initializeLogging(summaryLogFileName, true);
        logger = initializeLogging( this.getName() + "-on-" + this.id + "-on-tcpPort" + tcpPort + ":REGULAR",true);


        setupHTTP();
        setMessagingThreads();
        setComplimentaryThreads();
    }
    private void setupHTTP() throws IOException {
        this.httpServer = HttpServer.create(new InetSocketAddress(udpPort + 1), 0);
        this.httpServer.createContext("/log/verbose", exchange -> {
            try {
                // Construct the path the same way LoggingServer does
                String filePath = logDirectory + File.separator + verboseLogFileName + "-Log.txt";
                File logFile = new File(filePath);

                if (!logFile.exists()) {
                    String error = "Verbose log file not found at: " + filePath;
                    byte[] bytes = error.getBytes();
                    exchange.sendResponseHeaders(404, bytes.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(bytes);
                    }
                    return;
                }

                byte[] logContent = Files.readAllBytes(logFile.toPath());
                exchange.getResponseHeaders().set("Content-Type", "text/plain");
                exchange.sendResponseHeaders(200, logContent.length);

                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(logContent);
                }
            } catch (IOException e) {
                String error = "Error reading verbose log: " + e.getMessage();
                byte[] bytes = error.getBytes();
                exchange.sendResponseHeaders(500, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
        });

        this.httpServer.createContext("/log/summary", exchange -> {
            try {
                String filePath = logDirectory + File.separator + summaryLogFileName + "-Log.txt";
                File logFile = new File(filePath);

                if (!logFile.exists()) {
                    String error = "Summary log file not found at: " + filePath;
                    byte[] bytes = error.getBytes();
                    exchange.sendResponseHeaders(404, bytes.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(bytes);
                    }
                    return;
                }
                byte[] logContent = Files.readAllBytes(logFile.toPath());
                exchange.getResponseHeaders().set("Content-Type", "text/plain");
                exchange.sendResponseHeaders(200, logContent.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(logContent);
                }
            } catch (IOException e) {
                String error = "Error reading summary log: " + e.getMessage();
                byte[] bytes = error.getBytes();
                exchange.sendResponseHeaders(500, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
        });
        httpServer.start();
    }
    private void setComplimentaryThreads() {
        this.failedPeers = ConcurrentHashMap.newKeySet();
        this.leaderElection = new LeaderElection(this, this.LEADERELECTIONincomingMessages, failedPeers);
        this.heartbeatThread = new Thread(new HeartbeatManager(
                failedPeers,
                peerIDtoAddress,
                HEARTBEATincomingMessages,
                UDPoutgoingMessages,
                id,
                deadNodeMonitor,
                this.udpAddress,
                summaryLogger
        ));
        if (!(this instanceof GatewayPeerServerImpl)) {
            Map<String,String> uncompletedResults = new ConcurrentHashMap<>();

            this.leaderMonitorThread = new LeaderMonitorThread();
            leaderMonitorThread.setDaemon(true);
            try {
                this.tcpServerSocket = new ServerSocket();
                this.tcpServerSocket.setReuseAddress(true);
                this.tcpServerSocket.bind(new InetSocketAddress(this.udpPort + 2));
            } catch (IOException e) {
                throw new RuntimeException("Failed to create TCP ServerSocket", e);
            }
            this.leaderClass = new RoundRobinLeader(this, this.gateAwayID, peerIDtoAddress, failedPeers, uncompletedResults,tcpServerSocket);
            this.followerClass = new JavaRunnerFollower(this, uncompletedResults,tcpServerSocket);
        }
    }
    private void setMessagingThreads() throws IOException {
        this.UDPoutgoingMessages = new LinkedBlockingQueue<>();
        this.UDPincomingMessages = new LinkedBlockingQueue<>();

        this.LEADERELECTIONincomingMessages = new LinkedBlockingQueue<>();
        this.HEARTBEATincomingMessages = new LinkedBlockingQueue<>();

        this.udpMessageSender = new UDPMessageSender(UDPoutgoingMessages,this.udpPort);
        this.udpMessageReceiver = new UDPMessageReceiver(UDPincomingMessages, udpAddress, udpPort, this);
        this.dispatcherThread = new DispatcherThread();
        this.dispatcherThread.setDaemon(true);
        myHostName = udpAddress.getHostName();

    }
    private void startWorkerDaemonThreads() {
        this.dispatcherThread.start();
        this.udpMessageSender.start();
        this.udpMessageReceiver.start();
        this.heartbeatThread.start();
        this.leaderMonitorThread.start();
    }

    @Override
    public void run(){
        //step 1: create and run thread that sends broadcast messages
        //step 2: create and run thread that listens for messages sent to this server
        logger.log(Level.INFO,"Starting PeerServer....");
        startWorkerDaemonThreads();
        //step 3: main server loop
        while (!this.shutdown.get()){
            switch (getPeerState()){
                case LOOKING:
                    //start leader election, set leader to the election winner
                    logger.log(Level.INFO,"Looking for Leader....");
                    this.currentLeader = this.leaderElection.lookForLeader();
                    if (this.currentLeader == null) {
                        logger.log(Level.SEVERE, "Election failed to produce a leader!");
                        continue;
                    }
                    peerEpoch++; //increment the epoch after we found the leader.
                    this.state = this.currentLeader.getProposedLeaderID() == this.id ? ServerState.LEADING : ServerState.FOLLOWING;
                    logger.log(Level.INFO,"Found! Leader: " + this.currentLeader + "Epoch: " + this.peerEpoch + "My state: " + this.state);
                    break;
                case FOLLOWING:
                    logger.log(Level.INFO,"Doing Work!");
                    work();
                    //PERFORM FOLLOWING LOGIC,
                    break;
                case LEADING:
                    logger.log(Level.INFO,"Leading!");
                    lead();
                    break;
            }
        }
    }

    private void work(){
        this.followerClass.run();

    }
    private void lead(){
        this.leaderClass.run();
    }


    @Override
    public void shutdown(){
        this.shutdown.set(true);
        if(this.tcpServerSocket != null && !this.tcpServerSocket.isClosed()) {
            try {
                this.tcpServerSocket.close();
            } catch (IOException ignore) {
            }
        }

        this.udpMessageSender.shutdown();
        if(this.udpMessageReceiver != null)
            this.udpMessageReceiver.shutdown();
        if(this.leaderClass != null)
            this.leaderClass.shutdown();
        if(this.followerClass != null)
            this.followerClass.shutdown();
        this.heartbeatThread.interrupt();
        try {
            this.heartbeatThread.join(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;
    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        //for sending a message, we just need to add the message to the queue, and the sending worker thread will just poll and send it
        Message message = new Message(type,messageContents,this.myHostName,this.udpPort, target.getHostName(), target.getPort());
        try {
            this.UDPoutgoingMessages.put(message); //adds on the queue, worker should just get it and send it
        } catch (InterruptedException e) {
            //thread interrupted while sending message.

        }
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        //loop through all our peers
        for (Map.Entry<Long,InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if(entry.getValue() == udpAddress)
                continue;
            sendMessage(type,messageContents,entry.getValue());
        }
    }

    @Override
    public ServerState getPeerState() {
        return this.state;
    }

    @Override
    public void setPeerState(ServerState newState) {
        String logVal = "[" + this.id + "] Switching from [" + this.state + "] to [" + newState + "]";
        summaryLogger.log(Level.INFO, logVal);
        this.state = newState;
    }

    @Override
    public Long getServerId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.udpAddress;
    }

    @Override
    public int getUdpPort() {
        return this.udpPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return this.peerIDtoAddress.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        int totalServers = peerIDtoAddress.size();
        int votingServers = totalServers - numberOfObservers;
        int activeVotingServers = votingServers - this.failedPeers.size();
        return activeVotingServers / 2 + 1;

//        return (peerIDtoAddress.size() + 1 - numberOfObservers - this.failedPeers.size()) / 2 + 1; //take into account the quorom calculation that we need

    }

    @Override
    public boolean isPeerDead(long peerID) {
        return this.failedPeers.contains(peerID);
    }

    @Override
    public void reportFailedPeer(long peerID) {
        this.failedPeers.add(peerID);
    }

    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        Long id = null;
        for(Map.Entry<Long,InetSocketAddress> entry : peerIDtoAddress.entrySet()){
            InetSocketAddress inet = entry.getValue();
            if(inet.getHostName().equals(address.getHostName()) && inet.getPort() == address.getPort()){
                id = entry.getKey();
                break;
            }
        }
        if(id == null)
            return false;
        return failedPeers.contains(id);
    }
}