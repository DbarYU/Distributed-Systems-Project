package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HeartbeatManager implements Runnable{
    private class Entry {
        int heartbeatCounterForTheProcess;
        long timeOnReceivingNode;
        boolean failed;
        Entry(int heartbeatCounterForTheProcess, long timeOnReceivingNode,boolean failed) {
            this.heartbeatCounterForTheProcess = heartbeatCounterForTheProcess;
            this.timeOnReceivingNode = timeOnReceivingNode;
            this.failed = failed;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "heartbeatCounterForTheProcess=" + heartbeatCounterForTheProcess +
                    ", timeOnReceivingNode=" + timeOnReceivingNode +
                    ", failed=" + failed +
                    '}';
        }
    }
    public class GossipSender implements Runnable{
        Map<Long,Entry> gossipTable;
        LinkedBlockingQueue<Message> outgoingMessages;
        AtomicInteger lamport;
        GossipSender(Map<Long,Entry> gossipTable, LinkedBlockingQueue<Message> outgoingMessages, AtomicInteger lamport){
            this.gossipTable = gossipTable;
            this.outgoingMessages = outgoingMessages;
            this.lamport = lamport;
        }
        @Override
        public void run() {
            while(!Thread.interrupted()){
                try{
                    Thread.sleep(GOSSIP);
                    checkForFailures();
                    checkForClear();
                    int val = lamport.incrementAndGet();
                    this.gossipTable.computeIfPresent(peerID, (k, v) -> {
                        v.heartbeatCounterForTheProcess = val;
                        v.timeOnReceivingNode = System.currentTimeMillis();
                        return v;
                    });
                    Message msg =  buildMsgContent();
                    if(msg != null)
                        this.outgoingMessages.add(msg);
                    //SEND OUT A A GOSSIP OF OUR TABLE. EVERY TIME WE SEND OUT A MESSAGE WE NEED TO INCREMENT OUR LAMPORT CLOCK THOUGH.
                }catch (InterruptedException e){
                    break;
                }
            }
        }

        private Message buildMsgContent() {
            List<Map.Entry<Long, Entry>> entriesToSend = new ArrayList<>(gossipTable.entrySet());
            int count = entriesToSend.size();
            byte[] payload = new byte[4 + (count * 13)];
            ByteBuffer buffer = ByteBuffer.wrap(payload);

            buffer.putInt(count);

            for (Map.Entry<Long, Entry> entry : entriesToSend) {
                buffer.putLong(entry.getKey());
                buffer.putInt(entry.getValue().heartbeatCounterForTheProcess);
                buffer.put((byte) (entry.getValue().failed ? 1 : 0));
            }

            List<InetSocketAddress> activePeers = peerIDtoAddress.entrySet().stream()
                    .filter(e -> e.getKey() != peerID)
                    .filter(e -> !threadSafeDeadNodesSet.contains(e.getKey())) //dont include the dead nodes
                    .map(Map.Entry::getValue)
                    .toList();

            List<InetSocketAddress> targets = new ArrayList<>(activePeers);
            if (targets.isEmpty()) return null;

            InetSocketAddress target = targets.get(new Random().nextInt(targets.size()));

            return new Message(Message.MessageType.GOSSIP, buffer.array(), peerHost, peerPort, target.getHostName(), target.getPort());
        }
        private void checkForFailures(){
            for(Map.Entry<Long,Entry> entry : gossipTable.entrySet()){
                if(entry.getKey() == peerID)
                    continue;
                long currTime = System.currentTimeMillis();

                if(currTime - entry.getValue().timeOnReceivingNode > FAIL){
                    synchronized (deadNodeMonitor){
                        entry.getValue().failed = true;
                        threadSafeDeadNodesSet.add(entry.getKey());
                        deadNodeMonitor.notifyAll();
                    }
                    // we add it so the peerImpl can see who is out and who is in
                    //time has passed such that we haven't heard from this node, we need to mark it fail.
                    //we have a monitor that notifies the running node that node a is dead

                }
            }
        }
        private void checkForClear(){
            List<Long> keysToBeRemoved = new ArrayList<>();
            for(Map.Entry<Long,Entry> entry : gossipTable.entrySet()){
                if(!entry.getValue().failed)
                    continue;

                long currTime = System.currentTimeMillis();
                if(currTime- entry.getValue().timeOnReceivingNode > CLEANUP){
                    keysToBeRemoved.add(entry.getKey());
                    String log = "[" + peerID +"] no heartbeat from server [" + entry.getKey() +"]-SERVER FAILED";
                    summaryLogger.log(Level.INFO,log);
                    System.out.println(log);
                    //time has passed such that we haven't heard from this node, we need to mark it fail.
                }
            }
            for(Long key : keysToBeRemoved){
                gossipTable.remove(key);
            }
        }

    }
    public class GossipReceiver implements Runnable {
        Map<Long, Entry> gossipTable;
        LinkedBlockingQueue<Message> incomingMessages;
        AtomicInteger lamport;

        GossipReceiver(Map<Long, Entry> gossipTable, LinkedBlockingQueue<Message> incomingMessages, AtomicInteger lamport) {
            this.gossipTable = gossipTable;
            this.incomingMessages = incomingMessages;
            this.lamport = lamport;

        }

        @Override
        public void run() {

            while (!Thread.interrupted()) {
                try {
                    Message message = incomingMessages.take();

                    if (message.getMessageType() != Message.MessageType.GOSSIP) //stale, we run the heartbeat during work, not during election.
                        continue;

                    decodeMessageAndUpdateTable(message);
                    lamport.incrementAndGet();
                    this.gossipTable.computeIfPresent(peerID, (k, v) -> {
                        v.heartbeatCounterForTheProcess = lamport.get();
                        return v;
                    });
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
        }

        void decodeMessageAndUpdateTable(Message message) {
            byte[] bytes = message.getMessageContents();
            if (bytes == null || bytes.length < 4) return;

            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            int count = buffer.getInt();
            long now = System.currentTimeMillis();
            if (count < 0) return;

            for (int i = 0; i < count; i++) {
                if (buffer.remaining() < 13) break;

                long senderID = -1;
                for(Map.Entry<Long,InetSocketAddress> entry: peerIDtoAddress.entrySet()){
                    if((entry.getValue().getPort() == message.getSenderPort()))
                        senderID = entry.getKey();
                }
                long finalSenderID = senderID;
                long id = buffer.getLong();
                int counter = buffer.getInt();
                boolean failed = buffer.get() == 1;
                if (id == peerID || failed || threadSafeDeadNodesSet.contains(id)) continue;

                if (!gossipTable.containsKey(id)) {
                    String log = "[" + peerID +"] learned about new node ["  +id +"]";
                    summaryLogger.log(Level.INFO,log);
                    System.out.println(log);
                    Entry newEntry = new Entry(counter, now, false);
                    gossipTable.put(id, newEntry);
                }

                Entry existing = gossipTable.get(id);
                if (existing.failed) continue;
                gossipTable.computeIfPresent(id, (key, oldVal) -> {
                    if (counter > oldVal.heartbeatCounterForTheProcess) {
                        String log = "[" + peerID + "]: updated ["+ id+"] heartbeat sequence to [" + counter +"] based on message from [" + finalSenderID +"] at node time [" +now +"]" ;
                        summaryLogger.log(Level.INFO,log);
                        System.out.println(log);
                        return new Entry(counter, now, false);
                    }
                    return oldVal;
                });
            }
        }
    }
    static final int GOSSIP = 1000;
    static final int FAIL = GOSSIP * 20;
    static final int CLEANUP = FAIL * 2;


    Set<Long> threadSafeDeadNodesSet;
    LinkedBlockingQueue<Message> incomingMessages;
    LinkedBlockingQueue<Message> outgoingMessages;
    volatile Map<Long,Entry> gossipTable;
    private AtomicInteger lamportClock;
    Thread gossipRecieverThread;
    Map<Long, InetSocketAddress> peerIDtoAddress;
    String peerHost;
    int peerPort;
    long peerID;
    Thread gossipSenderThread;
    final Object deadNodeMonitor;
    Logger summaryLogger;
    HeartbeatManager(Set<Long> threadSafeDeadNodesSet, Map<Long, InetSocketAddress> peerIDtoAddress, LinkedBlockingQueue<Message> incomingMessages,LinkedBlockingQueue<Message> outgoingMessages,long peerID,Object deadNodeMonitor,InetSocketAddress peerHost, Logger logger){
        //this accepts a threadSafeset which the thread will keep updating everytime a node becomes "dead"
        //need the original peerIDtoAddress so we know who to send messages to.
        //have our queues to send / recieve messages.
        this.threadSafeDeadNodesSet = threadSafeDeadNodesSet;
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.lamportClock = new AtomicInteger();
        this.gossipTable = new ConcurrentHashMap<>();

        this.summaryLogger = logger;
        gossipTable.put(peerID, new Entry(0,System.currentTimeMillis(),false));

        this.gossipSenderThread = new Thread(new GossipSender(gossipTable,outgoingMessages,lamportClock));
        gossipSenderThread.setDaemon(true);
        this.gossipRecieverThread = new Thread(new GossipReceiver(gossipTable,incomingMessages,lamportClock));
        gossipRecieverThread.setDaemon(true);
        this.peerIDtoAddress = peerIDtoAddress;

        this.peerHost = peerHost.getHostName();
        this.peerPort = peerHost.getPort();

        this.peerID = peerID;
        this.deadNodeMonitor = deadNodeMonitor;

    }

    @Override
    public void run() {
        try {
            gossipRecieverThread.start();
            gossipSenderThread.start();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    gossipRecieverThread.join();
                    gossipSenderThread.join();
                } catch (InterruptedException e) {
                    gossipRecieverThread.interrupt();
                    gossipSenderThread.interrupt();
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } finally {
            gossipRecieverThread.interrupt();
            gossipSenderThread.interrupt();
        }
    }
}
