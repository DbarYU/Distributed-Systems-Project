package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.PeerServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.InetSocketAddress;
import java.util.*;

import edu.yu.cs.com3800.*;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Stage3Test {


    public static class ClientSim {

        private final int clientPort;
        private final InetSocketAddress clientAddress;
        private final LinkedBlockingQueue<Message> outgoingMessages;
        private final LinkedBlockingQueue<Message> incomingMessages;
        private final UDPMessageSender sender;
        private final UDPMessageReceiver receiver;

        public ClientSim(int clientPort) throws Exception {
            this.clientPort = clientPort;
            this.clientAddress = new InetSocketAddress("localhost", clientPort);
            this.outgoingMessages = new LinkedBlockingQueue<>();
            this.incomingMessages = new LinkedBlockingQueue<>();


            this.sender = new UDPMessageSender(outgoingMessages, clientPort);
            this.receiver = new UDPMessageReceiver(incomingMessages, clientAddress, clientPort, null);

            Util.startAsDaemon(sender, "Sender Thread");
            Util.startAsDaemon(receiver, "Receiver Thread");
        }


        public void sendWorkRequest(String javaCode, String leaderHost, int leaderPort)
                throws InterruptedException {
            Message workMessage = new Message(
                    Message.MessageType.WORK,
                    javaCode.getBytes(),
                    clientAddress.getHostString(),
                    clientPort,
                    leaderHost,
                    leaderPort
            );
            outgoingMessages.put(workMessage);
        }
        public void sendWorkRequest(String javaCode, String leaderHost, int leaderPort,long requestID) throws InterruptedException {
            Message workMessage = new Message(
                    Message.MessageType.WORK,
                    javaCode.getBytes(),
                    clientAddress.getHostString(),
                    clientPort,
                    leaderHost,
                    leaderPort,
                    requestID
            );
            outgoingMessages.put(workMessage);
        }

        public Message receiveResponse(int timeoutSeconds) throws InterruptedException {
            Message response = incomingMessages.poll(timeoutSeconds, TimeUnit.SECONDS);
            if (response != null) {
                System.out.println("Received response with request ID: " + response.getRequestID());
                System.out.println("Response content:\n" + new String(response.getMessageContents()));
            }
            return response;
        }
        public List<Message> receiveRequests() throws InterruptedException {
            return incomingMessages.stream().toList();
        }
        public void shutdown() throws InterruptedException {
            this.sender.shutdown();
            this.receiver.shutdown();
        }
    }


    private List<PeerServerImpl> servers;
    private List<PeerServerImpl> clients;
    Map<String,String> codeToResponseMap;
    String VALID_HELLO_WORLD = """
                        public class HelloWorld {
                            public String run() {
                                return "Hello from client!";
                            }
                        }
                        """;
    String VALID_HELLO_WORLD_RESPONSE = "Hello from client!";
    String INVALID_CODE = "HELLO FROM client!";
    String INVALID_RESPONSE = "Error getting Message: No class name found in code";




    @BeforeEach
    public void setUp() {
        servers = new ArrayList<>();
        this.codeToResponseMap = new LinkedHashMap<>();


        for (int i = 1; i <= 100; i++) {
            String code = String.format("""
            public class TestRunner {
                public TestRunner() {
                }
                public String run() {
                    return "%d";
                }
            }
            """, i);
            codeToResponseMap.put(String.valueOf(i),code);
        }

    }

    @AfterEach
    public void tearDown() throws Exception {
        if (servers != null) {
            for (PeerServerImpl server : servers) {
                try {
                    server.shutdown();
                } catch (Exception e) {
                    System.err.println("Error shutting down server " + server.getServerId() + ": " + e.getMessage());
                }
            }
            servers.clear();
        }
        Thread.sleep(2000);
    }

    @Test
    public void testElectionAndGenerateWorkWith3Servers() throws Exception {

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 8010));
        peerMap.put(2L, new InetSocketAddress("localhost", 8020));
        peerMap.put(3L, new InetSocketAddress("localhost", 8030));

        long expectedLeader = 3L;

        servers = createServers(peerMap);
        startServers(servers);
        Thread.sleep(7000);
        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader,server.getCurrentLeader().getProposedLeaderID());
        }

        //after we asserted that this is the leader,
        //lets assign work from a client and assert that it has been completed.
        ClientSim client = new ClientSim(10003);
        client.sendWorkRequest(this.VALID_HELLO_WORLD,peerMap.get(expectedLeader).getHostString(),peerMap.get(expectedLeader).getPort());

        Message res = client.receiveResponse(3000);
        String output = new String(res.getMessageContents());
        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, output);
        for(PeerServer server : servers) {
            server.shutdown();
        }
        client.shutdown();


    }
    @Test
    public void testElectionAndGenerateWorkWith5Servers() throws Exception {

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 7010));
        peerMap.put(2L, new InetSocketAddress("localhost", 7020));
        peerMap.put(3L, new InetSocketAddress("localhost", 7030));
        peerMap.put(4L, new InetSocketAddress("localhost", 7040));
        peerMap.put(5L, new InetSocketAddress("localhost", 7050));

        long expectedLeader = 5L;

        servers = createServers(peerMap);
        startServers(servers);
        Thread.sleep(7000);
        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader,server.getCurrentLeader().getProposedLeaderID());
        }

        //after we asserted that this is the leader,
        //lets assign work from a client and assert that it has been completed.
        ClientSim client = new ClientSim(10002);
        client.sendWorkRequest(this.VALID_HELLO_WORLD,peerMap.get(expectedLeader).getHostString(),peerMap.get(expectedLeader).getPort());

        Message res = client.receiveResponse(3000);
        String output = new String(res.getMessageContents());
        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, output);
        for(PeerServer server : servers) {
            server.shutdown();
        }
        client.shutdown();

    }
    @Test
    public void testElectionAndGenerateWorkWith7Servers() throws Exception {

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 9010));
        peerMap.put(2L, new InetSocketAddress("localhost", 9020));
        peerMap.put(3L, new InetSocketAddress("localhost", 9030));
        peerMap.put(4L, new InetSocketAddress("localhost", 9040));
        peerMap.put(5L, new InetSocketAddress("localhost", 9050));
        peerMap.put(6L, new InetSocketAddress("localhost", 9060));
        peerMap.put(7L, new InetSocketAddress("localhost", 9070));


        long expectedLeader = 7L;

        servers = createServers(peerMap);
        startServers(servers);
        Thread.sleep(7000);
        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader,server.getCurrentLeader().getProposedLeaderID());
        }

        //after we asserted that this is the leader,
        //lets assign work from a client and assert that it has been completed.
        ClientSim client = new ClientSim(10001);

        client.sendWorkRequest(this.VALID_HELLO_WORLD,peerMap.get(expectedLeader).getHostString(),peerMap.get(expectedLeader).getPort());

        Message res = client.receiveResponse(3000);
        String output = new String(res.getMessageContents());
        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, output);
        for(PeerServer server : servers) {
            server.shutdown();
        }
        client.shutdown();
    }
    @Test
    public void testElectionAndGenerateWith5ServersStaggeredStart() throws Exception {
        //stagger the start for each server.

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 12010));
        peerMap.put(2L, new InetSocketAddress("localhost", 12020));
        peerMap.put(3L, new InetSocketAddress("localhost", 12030));
        peerMap.put(4L, new InetSocketAddress("localhost", 12040));
        peerMap.put(5L, new InetSocketAddress("localhost", 12050));

        servers = createServers(peerMap);

        for (PeerServerImpl server : servers) {
            Thread thread = new Thread(server, String.valueOf(server.getServerId()));
            thread.start();
            Thread.sleep(2000);
        }

        Thread.sleep(5000);

        for(PeerServer server : servers) {
            Assertions.assertEquals(5L, server.getCurrentLeader().getProposedLeaderID());
        }
        ClientSim client = new ClientSim(10009);

        client.sendWorkRequest(this.VALID_HELLO_WORLD,peerMap.get(5L).getHostString(),peerMap.get(5L).getPort());

        Message res = client.receiveResponse(3000);
        String output = new String(res.getMessageContents());
        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, output);
        for(PeerServer server : servers) {
            server.shutdown();
        }
        client.shutdown();
    }
    @Test
    public void testClientSendingMany() throws Exception {
        //stagger the start for each server.

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 13010));
        peerMap.put(2L, new InetSocketAddress("localhost", 13020));
        peerMap.put(3L, new InetSocketAddress("localhost", 13030));


        servers = createServers(peerMap);

        for (PeerServerImpl server : servers) {
            Thread thread = new Thread(server, String.valueOf(server.getServerId()));
            thread.start();
            Thread.sleep(2000);
        }

        Thread.sleep(5000);

        for(PeerServer server : servers) {
            Assertions.assertEquals(3L, server.getCurrentLeader().getProposedLeaderID());
        }
        ClientSim client = new ClientSim(10009);

        for(Map.Entry<String , String> entry : this.codeToResponseMap.entrySet()){
            client.sendWorkRequest(entry.getValue(),peerMap.get(3L).getHostString(),peerMap.get(3L).getPort(),Long.parseLong(entry.getKey()));
        }
        Thread.sleep(5000);

        List<Message> res = client.receiveRequests();
        Assertions.assertEquals(this.codeToResponseMap.size(), res.size());
        for(Message message : res){
            Assertions.assertEquals(new String(message.getMessageContents()), String.valueOf(message.getRequestID()));
        }
        for(PeerServer server : servers) {
            server.shutdown();
        }
        client.shutdown();
    }
    @Test
    public void testClientSendingManyButSomeAreAssignedToWorkers() throws Exception {
        //stagger the start for each server.

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 13010));
        peerMap.put(2L, new InetSocketAddress("localhost", 13020));
        peerMap.put(3L, new InetSocketAddress("localhost", 13030));


        servers = createServers(peerMap);

        for (PeerServerImpl server : servers) {
            Thread thread = new Thread(server, String.valueOf(server.getServerId()));
            thread.start();
            Thread.sleep(2000);
        }

        Thread.sleep(5000);

        for(PeerServer server : servers) {
            Assertions.assertEquals(3L, server.getCurrentLeader().getProposedLeaderID());
        }
        ClientSim client = new ClientSim(10009);

        int count = 0;
        for(Map.Entry<String , String> entry : this.codeToResponseMap.entrySet()){
            if(count % 2 == 0)
                client.sendWorkRequest(entry.getValue(),peerMap.get(2L).getHostString(),peerMap.get(2L).getPort(),Long.parseLong(entry.getKey()));
            else
                client.sendWorkRequest(entry.getValue(),peerMap.get(3L).getHostString(),peerMap.get(3L).getPort(),Long.parseLong(entry.getKey()));

            count++;

        }
        Thread.sleep(5000);

        List<Message> res = client.receiveRequests();
        Assertions.assertEquals(this.codeToResponseMap.size() / 2, res.size());
        for(Message message : res){
            Assertions.assertTrue(message.getRequestID() % 2 == 0);
            Assertions.assertEquals(new String(message.getMessageContents()), String.valueOf(message.getRequestID()));
        }
        for(PeerServer server : servers) {
            server.shutdown();
        }
        client.shutdown();
    }
    @Test
    public void testManyClientsSending() throws Exception {
        //stagger the start for each server.

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 13010));
        peerMap.put(2L, new InetSocketAddress("localhost", 13020));
        peerMap.put(3L, new InetSocketAddress("localhost", 13030));
        peerMap.put(4L, new InetSocketAddress("localhost", 13040));
        peerMap.put(5L, new InetSocketAddress("localhost", 13050));


        servers = createServers(peerMap);

        for (PeerServerImpl server : servers) {
            Thread thread = new Thread(server, String.valueOf(server.getServerId()));
            thread.start();
            Thread.sleep(2000);
        }

        Thread.sleep(5000);

        for(PeerServer server : servers) {
            Assertions.assertEquals(5L, server.getCurrentLeader().getProposedLeaderID());
        }
        ClientSim client1 = new ClientSim(10009);
        ClientSim client2 = new ClientSim(10010);
        ClientSim client3 = new ClientSim(10011);

        client1.sendWorkRequest(this.VALID_HELLO_WORLD,peerMap.get(5L).getHostString(),peerMap.get(5L).getPort());
        client2.sendWorkRequest(this.VALID_HELLO_WORLD,peerMap.get(5L).getHostString(),peerMap.get(5L).getPort());
        client3.sendWorkRequest(this.VALID_HELLO_WORLD,peerMap.get(5L).getHostString(),peerMap.get(5L).getPort());


        Message res1 = client1.receiveResponse(3000);
        Message res2 = client2.receiveResponse(3000);
        Message res3 = client3.receiveResponse(3000);

        String output1 = new String(res1.getMessageContents());
        String output2 = new String(res2.getMessageContents());
        String output3 = new String(res3.getMessageContents());

        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, output1);
        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, output2);
        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, output3);

        for(PeerServer server : servers) {
            server.shutdown();
        }
        client1.shutdown();
        client2.shutdown();
        client3.shutdown();
    }
    @Test
    public void testElectionAndGenerateInvalidWorkWith3Servers() throws Exception {

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 8010));
        peerMap.put(2L, new InetSocketAddress("localhost", 8020));
        peerMap.put(3L, new InetSocketAddress("localhost", 8030));

        long expectedLeader = 3L;

        servers = createServers(peerMap);
        startServers(servers);
        Thread.sleep(7000);
        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader,server.getCurrentLeader().getProposedLeaderID());
        }

        //after we asserted that this is the leader,
        //lets assign work from a client and assert that it has been completed.
        ClientSim client = new ClientSim(10003);
        client.sendWorkRequest(this.INVALID_CODE,peerMap.get(expectedLeader).getHostString(),peerMap.get(expectedLeader).getPort());

        Message res = client.receiveResponse(3000);
        String output = new String(res.getMessageContents());
        Assertions.assertEquals(this.INVALID_RESPONSE, output);
        for(PeerServer server : servers) {
            server.shutdown();
        }
        client.shutdown();


    }
    @Test
    public void testElectionAndGenerateInvalidAndValidWorkWith3Servers() throws Exception {

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 6010));
        peerMap.put(2L, new InetSocketAddress("localhost", 6020));
        peerMap.put(3L, new InetSocketAddress("localhost", 6030));

        long expectedLeader = 3L;

        servers = createServers(peerMap);
        startServers(servers);
        Thread.sleep(7000);
        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader,server.getCurrentLeader().getProposedLeaderID());
        }

        //after we asserted that this is the leader,
        //lets assign work from a client and assert that it has been completed.
        ClientSim client = new ClientSim(10003);
        client.sendWorkRequest(this.INVALID_CODE,peerMap.get(expectedLeader).getHostString(),peerMap.get(expectedLeader).getPort());
        client.sendWorkRequest(this.VALID_HELLO_WORLD,peerMap.get(expectedLeader).getHostString(),peerMap.get(expectedLeader).getPort());

        Thread.sleep(3000);
        List<Message> res = client.receiveRequests();
        boolean containsValid = false;
        boolean containsInvalid = false;
        for(Message message : res) {
            String output = new String(message.getMessageContents());
            if(output.equals(VALID_HELLO_WORLD_RESPONSE))
                containsValid = true;
            if(output.equals(INVALID_RESPONSE))
                containsInvalid = true;
        }
        Assertions.assertTrue(containsValid && containsInvalid);

        for(PeerServer server : servers) {
            server.shutdown();
        }
        client.shutdown();


    }





    private ArrayList<PeerServerImpl> createServers(HashMap<Long, InetSocketAddress> peerMap) {
        ArrayList<PeerServerImpl> serverList = new ArrayList<>();

        for (Map.Entry<Long, InetSocketAddress> entry : peerMap.entrySet()) {
            HashMap<Long, InetSocketAddress> otherPeers =
                    (HashMap<Long, InetSocketAddress>) peerMap.clone();
            otherPeers.remove(entry.getKey());

            PeerServerImpl server = new PeerServerImpl(
                    entry.getValue().getPort(),
                    0,
                    entry.getKey(),
                    otherPeers
            );

            serverList.add(server);
        }
        return serverList;
    }
    private void startServers(List<PeerServerImpl> servers) {
        for (PeerServerImpl server : servers){
            Thread thread = new Thread(server,String.valueOf(server.getServerId()));
            thread.start();
        }
    }



}