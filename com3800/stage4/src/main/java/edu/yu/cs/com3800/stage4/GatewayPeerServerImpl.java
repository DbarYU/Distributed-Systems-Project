package edu.yu.cs.com3800.stage4;


import edu.yu.cs.com3800.UDPMessageReceiver;
import edu.yu.cs.com3800.UDPMessageSender;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;


public class GatewayPeerServerImpl extends PeerServerImpl {

    GatewayServer gateAwayServer;
    CountDownLatch shutdownLatch;
    AtomicBoolean httpServer;
    private static final Logger logger = Logger.getLogger(GatewayPeerServerImpl.class.getName() + Thread.currentThread().getName());
    GatewayPeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) throws IOException{
        super(udpPort,peerEpoch,serverID,peerIDtoAddress,gatewayID,numberOfObservers);
        super.setPeerState(ServerState.OBSERVER);
        this.httpServer = new AtomicBoolean(false);
        this.shutdownLatch = new CountDownLatch(1);
        int tcpPort = this.udpPort + 2;
        String fileName = "logs/" + Globals.RUN_ID + "/edu.yu.cs.com3800.stage4.GatewayPeerServerImpl-on-"
                + this.id
                + "-on-tcpPort" + tcpPort
                + "-Log.log";
        FileHandler fh = new FileHandler(fileName, true);
        logger.addHandler(fh);
    }


    @Override
    public void setPeerState(ServerState newState) {
        if(this.getPeerState() == ServerState.OBSERVER)
            return;
        super.setPeerState(newState);
    }
    @Override
    public void run(){
        //step 1: create and run thread that sends broadcast messages
        //step 2: create and run thread that listens for messages sent to this server
        logger.log(Level.INFO,"Starting GatewayPeerServerImpl");
        startWorkerDaemonThreads(this.udpMessageSender, this.udpMessageReceiver);
            //start leader election, set leader to the election winner
        logger.log(Level.INFO,"Looking for Leader....");
        this.currentLeader = this.leaderElection.lookForLeader();
        peerEpoch++; //increment the epoch after we found the leader.
        logger.log(Level.INFO,"Found! Leader: " + this.currentLeader + "Epoch: " + this.peerEpoch + "My state: " + this.state);
        observe();

    }
    private void startWorkerDaemonThreads(UDPMessageSender senderWorker, UDPMessageReceiver receiverWorker) {
        senderWorker.start(); //already set to daemon, so we  just start the thread.
        receiverWorker.start();
    }
    private void observe() {
        logger.log(Level.INFO,"Observing...");
        if(!this.httpServer.getAndSet(true))
            this.gateAwayServer.startHttpServer();
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    @Override
    public void shutdown(){
        this.shutdownLatch.countDown();
    }
}
