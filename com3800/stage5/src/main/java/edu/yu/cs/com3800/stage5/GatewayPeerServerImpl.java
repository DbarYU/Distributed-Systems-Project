package edu.yu.cs.com3800.stage5;


import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.UDPMessageReceiver;
import edu.yu.cs.com3800.UDPMessageSender;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;


public class GatewayPeerServerImpl extends PeerServerImpl implements LoggingServer {

    GatewayServer gateAwayServer;
    CountDownLatch shutdownLatch;
    AtomicBoolean httpServer;


    private final Logger logger;
    GatewayPeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) throws IOException {
        super(udpPort, peerEpoch, serverID, peerIDtoAddress, gatewayID, numberOfObservers); //already sets the heartbeat
        super.setPeerState(ServerState.OBSERVER);
        this.httpServer = new AtomicBoolean(false);
        this.shutdownLatch = new CountDownLatch(1);
        int tcpPort = this.udpPort + 2;
        logger = initializeLogging(
                GatewayServer.class.getName() + "-on-" + this.id + "-on-tcpPort" + tcpPort +":REGULAR",true
        );

    }


    @Override
    public void setPeerState(ServerState newState) {
        if (this.getPeerState() == ServerState.OBSERVER)
            return;
        super.setPeerState(newState);
    }

    @Override
    public void run() {
        startWorkerDaemonThreads(this.udpMessageSender, this.udpMessageReceiver);
        logger.log(Level.INFO, "Starting GatewayPeerServerImpl");
        if (!this.httpServer.getAndSet(true)) {
            this.gateAwayServer.startHttpServer();
        }

        while (!Thread.currentThread().isInterrupted() && !this.shutdown.get()) {
            logger.log(Level.INFO, "Gateway looking for leader...");
            startLeaderELlection();
            while (!Thread.currentThread().isInterrupted() && this.currentLeader != null && !this.failedPeers.contains(this.currentLeader.getProposedLeaderID()) && !this.shutdown.get()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            logger.log(Level.INFO, "Gateway detected leader failure or restart. Re-running election.``");
        }
        shutdown();
    }
    private void startLeaderELlection(){
        logger.log(Level.INFO,"Looking for Leader....");
        this.currentLeader = null;
        this.currentLeader = this.leaderElection.lookForLeader();
        peerEpoch++; //increment the epoch after we found the leader.
        logger.log(Level.INFO,"Found! Leader: " + this.currentLeader + "Epoch: " + this.peerEpoch + "My state: " + this.state);
    }
    private void startWorkerDaemonThreads(UDPMessageSender senderWorker, UDPMessageReceiver receiverWorker) {
        this.dispatcherThread.start();
        senderWorker.start();
        receiverWorker.start();
        this.heartbeatThread.start();
    }
    @Override
    public void shutdown(){
        super.shutdown();
        this.shutdownLatch.countDown();

    }
}
