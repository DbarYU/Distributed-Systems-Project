package edu.yu.cs.com3800;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**We are implemeting a simplfied version of the election algorithm. For the complete version which covers all possible scenarios, see https://github.com/apache/zookeeper/blob/90f8d835e065ea12dddd8ed9ca20872a4412c78a/zookeeper-server/src/main/java/org/apache/zookeeper/server/quorum/FastLeaderElection.java#L913
 */
public class LeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 3200;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 30 seconds.
     */
    private final static int maxNotificationInterval = 30000;
    private final static int baseMaxNotificationInterval = 200;
    private PeerServer peerServer;
    private LinkedBlockingQueue<Message> incomingMessages;
    private Logger logger;
    private Vote currVote;
    Map<Long, ElectionNotification> votes;


    public LeaderElection(PeerServer server, LinkedBlockingQueue<Message> incomingMessages, Logger logger) {
        this.peerServer = server; //associated PeerServer to the Leader election class
        this.incomingMessages = incomingMessages;
        this.logger = logger;
        this.currVote = null;
        this.votes = new HashMap<>();
    }

    /**
     * Note that the logic in the comments below does NOT cover every last "technical" detail you will need to address to implement the election algorithm.
     * How you store all the relevant state, etc., are details you will need to work out.
     * @return the elected leader
     */
    public synchronized Vote lookForLeader() {
        try {
            currVote = new Vote(this.peerServer.getServerId(),this.peerServer.getPeerEpoch());
            sendNotifications();//first notification is to all the other servers sending a vote that I am the election
            this.votes.put(this.peerServer.getServerId(),
                    new ElectionNotification(this.peerServer.getServerId(),
                            this.peerServer.getPeerState(),
                            this.peerServer.getServerId(),
                            this.currVote.getPeerEpoch()));
            //send initial notifications to get things started
            //sendNotifications();
            //Loop in which we exchange notifications with other servers until we find a leader
            int delay = baseMaxNotificationInterval;
            while(true){
                //Remove next notification from queue
                //If no notifications received...
                delay = Math.min(delay, maxNotificationInterval);
                Message msg = incomingMessages.poll(delay, TimeUnit.MILLISECONDS);
                if (msg == null){
                    //...resend notifications to prompt a reply from others
                    sendNotifications(currVote.getProposedLeaderID());
                    //...use exponential back-off when notifications not received but no longer than maxNotificationInterval...
                    delay = delay * 2;
                 //If we did get a message...
                }else{
                    delay = baseMaxNotificationInterval;
                    //...if it's for an earlier epoch, or from an observer, ignore it.
                    ElectionNotification noti = getNotificationFromMessage(msg);
                    if(noti.getPeerEpoch() < this.peerServer.getPeerEpoch())
                        continue;
                    if(noti.getState() == PeerServer.ServerState.OBSERVER)
                        continue;
                    //...if the received message has a vote for a leader which supersedes mine, change my vote (and send notifications to all other voters about my new vote).
                    if(supersedesCurrentVote(noti.getProposedLeaderID(),noti.getPeerEpoch())) {

                        this.currVote = new Vote(noti.getProposedLeaderID(),noti.getPeerEpoch());
                        this.votes.put(this.peerServer.getServerId(),
                                new ElectionNotification(this.currVote.getProposedLeaderID(),
                                        this.peerServer.getPeerState(),
                                        this.peerServer.getServerId(),
                                        this.currVote.getPeerEpoch()));
                        //also add my own vote to the tally,
                        sendNotifications(this.currVote.getProposedLeaderID());
                    }
                    this.votes.put(noti.getSenderID(),noti);
                    //(Be sure to keep track of the votes I received and who I received them from.)
                    //If I have enough votes to declare my currently proposed leader as the leader...

                    //If there are no new relevant message from the reception queue, set my own state to either LEADING or FOLLOWING and RETURN the elected leader.
                    if(haveEnoughVotes(this.votes, this.currVote)){
                        long startTime = System.currentTimeMillis();
                        while(System.currentTimeMillis() - startTime < finalizeWait) {
                            if(!this.incomingMessages.isEmpty()) {
                                break;
                            }
                            Thread.sleep(10);
                        }
                        //wait for finalizewait time, if we dont get a message by then, this is our new leader.
                        //not ideal for busy waiting, but there is no API where i can insert it back in the front of the queue, therefor i wouldnt want to wait, take the msg then place it at the end.
                        //even though it wont change correctness.
                        if(this.incomingMessages.isEmpty()) {
                            return acceptElectionWinner(new ElectionNotification(
                                    this.currVote.getProposedLeaderID(),
                                    this.peerServer.getPeerState(),
                                    this.peerServer.getServerId(),
                                    this.currVote.getPeerEpoch()
                            ));
                        }
                    }
                }
                //..do a last check to see if there are any new votes for a higher ranked possible leader. If there are, continue in my election "while" loop.
            }
        }
        catch (Exception e) {
            this.logger.log(Level.SEVERE,"Exception occurred during election, election canceled",e);
        }
        this.logger.log(Level.INFO,"Leader election is in-complete");
        return null;
    }

    private void sendNotifications(){
        //send our initial notifications proclaiming I AM THE LEADER.
        ElectionNotification electionNotification = new ElectionNotification(
              this.peerServer.getServerId(),
              this.peerServer.getPeerState(),
              this.peerServer.getServerId(),
                this.peerServer.getPeerEpoch()
        );
        byte[] msgContent = buildMsgContent(electionNotification);
        this.peerServer.sendBroadcast(Message.MessageType.ELECTION, msgContent);
    }
    private void sendNotifications(long proposedLeaderID) {
        ElectionNotification notification = new ElectionNotification(
                proposedLeaderID,
                this.peerServer.getPeerState(),
                this.peerServer.getServerId(),
                this.peerServer.getPeerEpoch()
        );
        byte[] msgContent = buildMsgContent(notification);
        this.peerServer.sendBroadcast(Message.MessageType.ELECTION, msgContent);
    }
    private Vote acceptElectionWinner(ElectionNotification n) {
        //set my state to either LEADING or FOLLOWING
        //clear out the incoming queue before returning
        if(n.getProposedLeaderID() == this.peerServer.getServerId())
            this.peerServer.setPeerState(PeerServer.ServerState.LEADING);
        else
            this.peerServer.setPeerState(PeerServer.ServerState.FOLLOWING);

        this.incomingMessages.clear();
        this.votes.clear();
        return this.currVote;

    }
    /*
     * We return true if one of the following two cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        if(this.currVote == null)
            return true;
        return (newEpoch > this.currVote.getPeerEpoch()) || ((newEpoch == this.currVote.getPeerEpoch()) && (newId > this.currVote.getProposedLeaderID()));
    }

    /**
     * Termination predicate. Given a set of votes, determines if we have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote.
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        //is the number of votes for the proposal >= the size of my peer server’s quorum?

        int quorum = this.peerServer.getQuorumSize();
        int current = 0;
        for (ElectionNotification noti : votes.values()) {
            current = noti.getProposedLeaderID() ==  proposal.getProposedLeaderID() ? current + 1 : current;
        }
        return current >= quorum;
    }

    public static ElectionNotification getNotificationFromMessage(Message msg){
        ByteBuffer bytes = ByteBuffer.wrap(msg.getMessageContents()); //need to look how message encodes the noti so i can decode
        long peerEpoch = bytes.getLong();
        long senderID = bytes.getLong();
        long proposedLeaderID = bytes.getLong();
        char stateChar = bytes.getChar();

        PeerServer.ServerState state = PeerServer.ServerState.getServerState(stateChar);

        return new ElectionNotification(proposedLeaderID, state, senderID, peerEpoch);
    }

    public static byte[] buildMsgContent(ElectionNotification notification) {
        ByteBuffer buffer = ByteBuffer.allocate(26); //build messgae to send notis

        //ORDER IS IMPORTANT

        buffer.putLong(notification.getPeerEpoch());
        buffer.putLong(notification.getSenderID());
        buffer.putLong(notification.getProposedLeaderID());
        buffer.putChar(notification.getState().getChar());
        return buffer.array();
    }
}