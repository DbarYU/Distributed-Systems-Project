package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;



public class PeerServerImpl extends Thread implements PeerServer {
    private final InetSocketAddress myAddress;
    private final int myPort;
    private volatile ServerState state;
    private volatile boolean shutdown;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final Long id;

    private volatile Vote currentLeader;
    private final Map<Long,InetSocketAddress> peerIDtoAddress;

    private final UDPMessageSender senderWorker;
    private final UDPMessageReceiver receiverWorker;

    private final LeaderElection leaderElection;
    private final String myHostName;
    private static final Logger logger = Logger.getLogger("PeerServerImpl");

    private final int quorom;

    long epochs;

    JavaRunnerFollower followerClass;
    RoundRobinLeader leaderClass;



    public PeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress){
        //code here...
        this.myPort = myPort;
        this.myAddress = new InetSocketAddress("localhost",myPort);
        this.peerIDtoAddress = peerIDtoAddress; //addresses of all the other servers in th cluster
        this.id = id;
        this.state = ServerState.LOOKING; //currently looking for the current leader.
        this.outgoingMessages = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Message> incomingMessages = new LinkedBlockingQueue<>();
        this.currentLeader = null; //we dont have a leader set yet, set to null
        this.leaderElection = new LeaderElection(this, incomingMessages,logger);

        this.followerClass = new JavaRunnerFollower(this, incomingMessages, outgoingMessages,logger);
        this.leaderClass = new RoundRobinLeader(this,incomingMessages,outgoingMessages,peerIDtoAddress,logger);

        this.senderWorker = new UDPMessageSender(outgoingMessages,myPort);
        try {
            this.receiverWorker = new UDPMessageReceiver(incomingMessages, myAddress, myPort, this);
        }catch (IOException e){
            logger.log(Level.SEVERE,"Error occurred when initializing constructor: " + e.getMessage(),e);
            throw new RuntimeException("error occurred when initializing constructor");
        }
        myHostName = myAddress.getHostName();
        this.quorom = (peerIDtoAddress.size() + 1) / 2 + 1;
        this.epochs = peerEpoch;


    }
    @Override
    public void run(){
        logger.log(Level.INFO,"Peer Server started: " + this.id);
        //step 1: create and run thread that sends broadcast messages
        //step 2: create and run thread that listens for messages sent to this server
        startWorkerDaemonThreads(this.senderWorker, this.receiverWorker);
        //step 3: main server loop
        try{
            while (!this.shutdown){
                switch (getPeerState()){
                    case LOOKING:
                        //start leader election, set leader to the election winner
                        this.currentLeader = this.leaderElection.lookForLeader();
                        epochs++; //increment the epoch after we found the leader.
                        this.state = this.currentLeader.getProposedLeaderID() == this.id ? ServerState.LEADING : ServerState.FOLLOWING;
                        logger.log(Level.INFO,"Peer Server state: " + this.state);
                        logger.info("Peer server is following ID: " + this.currentLeader);
                        break;
                    case FOLLOWING:
                        logger.log(Level.INFO,"Doing work...");
                        work();
                        //PERFORM FOLLOWING LOGIC,
                        break;
                    case LEADING:
                        logger.log(Level.INFO,"Leading...");
                        lead();
                        //perfrom LEADING LOGIC.
                    break;
                }
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE,"Error occurred when starting thread",e);
        }
    }

    private void work(){
        this.followerClass.run();
    }
    private void lead(){
        this.leaderClass.run();
    }
    private void startWorkerDaemonThreads(UDPMessageSender senderWorker, UDPMessageReceiver receiverWorker) {
        senderWorker.start(); //already set to daemon, so we  just start the thread.
        receiverWorker.start();
    }

    @Override
    public void shutdown(){
        this.shutdown = true;
        this.senderWorker.shutdown();
        if(this.receiverWorker != null)
            this.receiverWorker.shutdown();
        if(this.leaderClass != null)
            this.leaderClass.shutdown();
        if(this.followerClass != null)
            this.followerClass.shutdown();
        logger.log(Level.INFO,"Peer Server shutdown: " + this.id);
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
        Message message = new Message(type,messageContents,this.myHostName,this.myPort, target.getHostName(), target.getPort());
        try {
            this.outgoingMessages.put(message); //adds on the queue, worker should just get it and send it
        } catch (InterruptedException e) {
            //thread interrupted while sending message.
            logger.log(Level.SEVERE,"Thread interrupted while sending message. aborting",e);

        }
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        //loop through all our peers
        logger.log(Level.INFO,"Peer Server sendBroadcast: " + this.id);
        for (Map.Entry<Long,InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if(entry.getValue() == myAddress)
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
        return this.epochs;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    @Override
    public int getUdpPort() {
        return this.myPort;
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
