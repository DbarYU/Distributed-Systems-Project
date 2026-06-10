package edu.yu.cs.com3800;


import java.io.IOException;
import java.net.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UDPMessageReceiver extends Thread implements LoggingServer {
    private static final int MAXLENGTH = 4096;
    private final InetSocketAddress myAddress;
    private final int myPort;
    private LinkedBlockingQueue<Message> incomingMessages;
    private PeerServer peerServer;
    private final Logger logger;
    public UDPMessageReceiver(LinkedBlockingQueue<Message> incomingMessages, InetSocketAddress myAddress, int myPort, PeerServer peerServer) throws IOException {
        this.incomingMessages = incomingMessages;
        this.myAddress = myAddress;
        this.myPort = myPort;
        this.setDaemon(true);
        this.peerServer = peerServer;
        int tcpPort = myPort + 2;
        this.logger = initializeLogging(UDPMessageReceiver.class.getName() + "-on-" + this.peerServer.getServerId() + "-on-tcpPort" + tcpPort +":REGULAR",false);

    }

    public void shutdown() {
        interrupt();
    }

    @Override
    public void run() {
        //create the socket
        DatagramSocket socket = null;
        logger.log(Level.INFO, "UDPMessageReceiver started");
        try {
            //System.out.println("DEBUG: Creating receiving socket on " + this.myAddress.getPort());
            socket = new DatagramSocket(this.myAddress);
            socket.setSoTimeout(3000);
        }
        catch (Exception e) {
            return;
        }
        //loop
        while (!this.isInterrupted()) {
            try {
                logger.log(Level.INFO, "UDPMessageReceiver receiving data");
                DatagramPacket packet = new DatagramPacket(new byte[MAXLENGTH], MAXLENGTH);
                socket.receive(packet); // Receive packet from a client
                Message received = new Message(packet.getData());
                InetSocketAddress sender = new InetSocketAddress(received.getSenderHost(), received.getSenderPort());
                //ignore messages from peers marked as dead
                if (this.peerServer != null && this.peerServer.isPeerDead(sender)) {
                    continue;
                }
                //this is logic required for stage 5...
                if (sendLeader(received)) {
                    Vote leader = this.peerServer.getCurrentLeader();
                    //might've entered election between the two previous lines of code, which would make leader null, hence must test
                    if(leader != null){
                        ElectionNotification notification = new ElectionNotification(leader.getProposedLeaderID(), this.peerServer.getPeerState(), this.peerServer.getServerId(), this.peerServer.getPeerEpoch());
                        byte[] msgContent = LeaderElection.buildMsgContent(notification);
                        sendElectionReply(msgContent, sender);
                    }
                //end stage 5 logic
                }else if(!this.strayElectionMessage(received)){
                    //use interrupt-safe version, i.e. offer
                    boolean done = false;
                    while(!done){
                        done = this.incomingMessages.offer(received);
                    }
                }
            }
            catch (SocketTimeoutException ste) {
                logger.log(Level.INFO, "UDPMessageReceiver receiving data timeout");
            }
            catch (Exception e) {
                if (!this.isInterrupted()) {
                }
            }
        }
        //cleanup
        if (socket != null) {
            socket.close();
        }
    }

    private void sendElectionReply(byte[] msgContent, InetSocketAddress target) {
        Message msg = new Message(Message.MessageType.ELECTION, msgContent, this.myAddress.getHostString(), this.myPort, target.getHostString(), target.getPort());
        try (DatagramSocket socket = new DatagramSocket()){
            byte[] payload = msg.getNetworkPayload();
            DatagramPacket sendPacket = new DatagramPacket(payload, payload.length, target);
            socket.send(sendPacket);
        }
        catch (IOException e) {
        }
    }

    /**
     * see if we got an Election LOOKING message while we are in FOLLOWING or LEADING
     * @param received
     * @return
     */
    private boolean sendLeader(Message received) {
        if (received.getMessageType() != Message.MessageType.ELECTION) {
            return false;
        }
        ElectionNotification receivedNotification = LeaderElection.getNotificationFromMessage(received);
        PeerServer.ServerState receivedState = receivedNotification.getState();
        if ((receivedState == PeerServer.ServerState.LOOKING || receivedState == PeerServer.ServerState.OBSERVER) && (this.peerServer.getPeerState() == PeerServer.ServerState.FOLLOWING || this.peerServer.getPeerState() == PeerServer.ServerState.LEADING)) {
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * if neither sender nor I am looking, and this is an election message, let it disappear
     * @param received
     * @return
     */
    private boolean strayElectionMessage(Message received) {
        if (received.getMessageType() != Message.MessageType.ELECTION) {
            return false;
        }
        ElectionNotification receivedNotification = LeaderElection.getNotificationFromMessage(received);
        if (receivedNotification.getState() != PeerServer.ServerState.LOOKING && this.peerServer.getCurrentLeader() != null) {
            return true;
        }
        else {
            return false;
        }
    }
}