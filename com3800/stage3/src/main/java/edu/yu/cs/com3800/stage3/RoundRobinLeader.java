package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.PeerServer;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RoundRobinLeader {


    //instance that is used by the leader to assing work to diff nodes.
    //have 2 threads,
    //1 thread that manages the assignment creation, because returning a result has nothing to do assigning work.
    PeerServer peerServer;
    LinkedBlockingQueue<Message> outgoingMessages;
    //keep track of the last server, and run a loop.
    List<InetSocketAddress> servers;
    int lastServer = -1;
    Random random = new Random();
    LinkedBlockingQueue<Message> incomingMessages;
    ConcurrentHashMap<Long,InetSocketAddress> work;
    Map<Long,Long> clientToInternal;
    Thread assignWorkThread;
    Thread getResultThread;
    Logger logger;
    long timeTorun = 3000;
    class assignWorkRunnable implements Runnable{
        @Override
        public void run() {
            logger.info("Starting assign work thread");
            while(!Thread.currentThread().isInterrupted()) {
                Message msg;
                try {
                     msg = incomingMessages.take();
                }catch (InterruptedException e) {
                    break;
                }
                if (msg.getMessageType() != Message.MessageType.WORK) {
                    incomingMessages.offer(msg);
                    continue;
                }
                logger.log(Level.INFO, "Received a work request: "+ msg);
                //we need to assign this work to one of the other peerservers
                long id = random.nextLong();
                clientToInternal.put(id,msg.getRequestID());
                assignWork(msg,id);
                work.put(id, new InetSocketAddress(msg.getSenderHost(), msg.getSenderPort()));
                logger.log(Level.INFO, "Assigned work, req id: " + id);
            }
            logger.info("Finished assign work thread");
        }
        private void assignWork(Message msg,long id){
            int index = lastServer ++;
            index = index %  servers.size();
            if(index < 0 )
                index = 0; //acount for integer oveflow
            InetSocketAddress address = servers.get(index);
            Message newMessage = new Message(Message.MessageType.WORK,msg.getMessageContents(),peerServer.getAddress().getHostName(),peerServer.getAddress().getPort(),address.getHostName(),address.getPort(),id);
            sendMessage(newMessage);
        }

    }
    class GetResultRunnable implements Runnable{
        @Override
        public void run() {
            logger.info("Starting getResult thread");
            while(!Thread.currentThread().isInterrupted()){
                Message completedWork;
                try {
                    completedWork = incomingMessages.take();
                } catch (InterruptedException e) {
                    break;
                }

                if(completedWork.getMessageType() != Message.MessageType.COMPLETED_WORK){
                    incomingMessages.offer(completedWork);
                    continue;
                }
                logger.log(Level.INFO, "Received a completed work request: "+ completedWork);
                //we now need to assign the completed work with the request ID
                InetSocketAddress client = work.get(completedWork.getRequestID());
                long clientRequestId = -1;
                if(client != null)
                    clientRequestId = clientToInternal.get(completedWork.getRequestID());

                Message newMessage = new Message(Message.MessageType.COMPLETED_WORK,completedWork.getMessageContents(),peerServer.getAddress().getHostName(),peerServer.getAddress().getPort(),client.getHostName(),client.getPort(),clientRequestId);
                sendMessage(newMessage);
            }
            logger.info("GetResult thread finished");
        }
    }

    public RoundRobinLeader(PeerServer peerServer, LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> outgoingMessages,Map<Long, InetSocketAddress> peerIDtoAddress, Logger logger) {
        this.peerServer = peerServer;
        servers = new ArrayList<>();
        for(InetSocketAddress address : peerIDtoAddress.values()){
            if(address.getHostName().equals(this.peerServer.getAddress().getHostName()) && address.getPort() == this.peerServer.getAddress().getPort())
                continue;
            servers.add(address);
        }
        this.incomingMessages = incomingMessages;
        this.work = new ConcurrentHashMap<>();
        this.outgoingMessages = outgoingMessages;
        this.logger = logger;
        this.clientToInternal = new HashMap<>();



    }
    public void run() {
        logger.log(Level.INFO, "RoundRobinLeader started!");
        this.assignWorkThread = new Thread(new assignWorkRunnable());
        this.getResultThread = new Thread(new GetResultRunnable());

        getResultThread.start();
        assignWorkThread.start();
        //run for a certain amount of time.

        try {
            Thread.sleep(this.timeTorun);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        //interrupt the threads -> your work is now done
        shutdown();
    }


    private void sendMessage(Message message){
        try {
            outgoingMessages.put(message); //adds on the queue, worker should just get it and send it
        } catch (InterruptedException e) {
            //thread interrupted while sending message.
            Thread.currentThread().interrupt();
        }
    }
    public void shutdown(){
            if(getResultThread != null)
                getResultThread.interrupt();
            if(assignWorkThread != null)
                assignWorkThread.interrupt();
            logger.log(Level.INFO, "Round robbing: Interrupting threads");
            try {
                if(getResultThread != null)
                    getResultThread.join();
                if(assignWorkThread != null)
                    assignWorkThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            logger.log(Level.INFO, "RoundRobinLeader stopped!");
    }
}
