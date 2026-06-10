package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;



public class PeerServerImpl extends Thread implements PeerServer {
    protected final InetSocketAddress udpAddress;
    protected final int udpPort;

    protected volatile ServerState state;
    protected volatile AtomicBoolean shutdown;

    protected final LinkedBlockingQueue<Message> outgoingMessages;
    protected final Long id;

    protected volatile Vote currentLeader;
    protected final ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress;

    protected final UDPMessageSender udpMessageSender;
    protected final UDPMessageReceiver udpMessageReceiver;

    protected final LeaderElection leaderElection;
    protected final String myHostName;

    protected final int quorom;
    protected final int numberOfObservers;

    long peerEpoch;
    long gateAwayID;

    JavaRunnerFollower followerClass;
    RoundRobinLeader leaderClass;
    GatewayServer observerClass;

    Logger logger = Logger.getLogger(PeerServer.class.getName());

    public PeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) throws IOException {
        //code here...
        this.udpPort = udpPort;
        this.numberOfObservers = numberOfObservers;
        this.udpAddress = new InetSocketAddress("localhost",udpPort);
        this.peerIDtoAddress = new ConcurrentHashMap<>(peerIDtoAddress); //addresses of all the other servers in th cluster
        this.id = serverID;
        this.state = ServerState.LOOKING; //currently looking for the current leader.
        this.outgoingMessages = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Message> incomingMessages = new LinkedBlockingQueue<>();
        this.currentLeader = null; //we dont have a leader set yet, set to null
        this.shutdown = new AtomicBoolean(false);
        this.leaderElection = new LeaderElection(this,incomingMessages);

        this.udpMessageSender = new UDPMessageSender(outgoingMessages,this.udpPort);
        this.udpMessageReceiver = new UDPMessageReceiver(incomingMessages, udpAddress, udpPort, this);
        myHostName = udpAddress.getHostName();
        this.quorom = (peerIDtoAddress.size() + 1 - numberOfObservers) / 2 + 1;
        this.peerEpoch = peerEpoch;
        this.gateAwayID = gatewayID;

        int tcpPort = this.udpPort + 2;
        String runID = Globals.RUN_ID;
        String fileName = "logs/" + runID + "/edu.yu.cs.com3800.stage4.PeerServerImpl-on-"
                + this.id
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
    @Override
    public void run(){
        //step 1: create and run thread that sends broadcast messages
        //step 2: create and run thread that listens for messages sent to this server
        logger.log(Level.INFO,"Starting PeerServer....");
        startWorkerDaemonThreads(this.udpMessageSender, this.udpMessageReceiver);
        //step 3: main server loop
        while (!this.shutdown.get()){
            switch (getPeerState()){
                case LOOKING:
                    //start leader election, set leader to the election winner
                    logger.log(Level.INFO,"Looking for Leader....");
                    this.currentLeader = this.leaderElection.lookForLeader();
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
        if(this.followerClass == null)
            this.followerClass = new JavaRunnerFollower(this);
        this.followerClass.run();

    }
    private void lead(){
        if(this.leaderClass == null)
            this.leaderClass = new RoundRobinLeader(this,this.gateAwayID,peerIDtoAddress);
        this.leaderClass.run();
    }

    private void startWorkerDaemonThreads(UDPMessageSender senderWorker, UDPMessageReceiver receiverWorker) {
        senderWorker.start(); //already set to daemon, so we  just start the thread.
        receiverWorker.start();
    }

    @Override
    public void shutdown(){
        this.shutdown.set(true);
        this.udpMessageSender.shutdown();
        if(this.udpMessageReceiver != null)
            this.udpMessageReceiver.shutdown();
        if(this.leaderClass != null)
            this.leaderClass.shutdown();
        if(this.followerClass != null)
            this.followerClass.shutdown();
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
            this.outgoingMessages.put(message); //adds on the queue, worker should just get it and send it
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
        return this.quorom;
    }



}
