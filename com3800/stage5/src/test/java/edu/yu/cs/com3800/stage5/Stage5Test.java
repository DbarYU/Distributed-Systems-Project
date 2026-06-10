package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.PeerServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.*;

import edu.yu.cs.com3800.*;

import java.util.concurrent.ConcurrentHashMap;
//DID THIS WITH JOSEPH COUZENS.
public class Stage5Test {


    public static class ClientSim {
        private final int gatewayHttpPort;
        private final String gatewayHost;

        boolean cached;

        public ClientSim(String gatewayHost, int gatewayHttpPort) {
            this.gatewayHost = gatewayHost;
            this.gatewayHttpPort = gatewayHttpPort;
        }


        public String sendWorkRequest(String javaCode) throws IOException {
            URL url = new URL("http://" + gatewayHost + ":" + gatewayHttpPort + "/compileandrun");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            try {
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Content-Type", "text/x-java-source");
                connection.setDoOutput(true);

                try (OutputStream os = connection.getOutputStream()) {
                    os.write(javaCode.getBytes());
                    os.flush();
                }

                int responseCode = connection.getResponseCode();
                String cachedHeader = connection.getHeaderField("Cached-Response");
                cached = "true".equalsIgnoreCase(cachedHeader);


                InputStream inputStream = (responseCode >= 200 && responseCode < 300)
                        ? connection.getInputStream()
                        : connection.getErrorStream();

                if (inputStream == null) {
                    throw new IOException("No response body received");
                }

                byte[] responseBytes = Util.readAllBytes(inputStream);
                String response = new String(responseBytes);

                if (responseCode >= 200 && responseCode < 300) {
                    return response;
                } else {
                    throw new IOException("HTTP error " + responseCode + ": " + response);
                }

            } finally {
                connection.disconnect();
            }
        }
    }




    private List<PeerServerImpl> servers;

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
    String INVALID_RESPONSE = "No class name found in code\\n";
    private final static long gateAwayID = 9999;
    private final static String gateAwayHost = "localhost";
    private final static int gateAwayPort = 8888;




    @BeforeEach
    public void setUp() {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        servers = new ArrayList<>();
        this.codeToResponseMap = new LinkedHashMap<>();


        for (int i = 1; i <= 30; i++) {
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
        peerMap.put(gateAwayID, new InetSocketAddress("localhost", 8040));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(gateAwayID);
        long expectedLeader = 3L;



        GatewayServer gatewayServer = new GatewayServer(8888,8040,0,gateAwayID,new ConcurrentHashMap<>(otherPeers),1);


        servers = createServers(peerMap);
        startServers(servers);
        gatewayServer.start();

        Thread.sleep(18000);

        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader,server.getCurrentLeader().getProposedLeaderID());
        }


        //after we asserted that this is the leader,
        //lets assign work from a client and assert that it has been completed.
        ClientSim client = new ClientSim(gateAwayHost,gateAwayPort);
        String res = client.sendWorkRequest(this.VALID_HELLO_WORLD);
        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, res);
        gatewayServer.shutdown();
        gatewayServer.join(3000);

        for(PeerServer server : servers) {
            server.shutdown();
        }

        for (PeerServerImpl server : servers) {
            server.join(5000);
        }
    }
    @Test
    public void testElectionAndKillLeader() throws Exception {
        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 9010));
        peerMap.put(2L, new InetSocketAddress("localhost", 9020));
        peerMap.put(3L, new InetSocketAddress("localhost", 9030));
        peerMap.put(4L, new InetSocketAddress("localhost", 9060));
        peerMap.put(5L, new InetSocketAddress("localhost", 9070)); //start 5.
        peerMap.put(gateAwayID, new InetSocketAddress("localhost", 9040));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(gateAwayID);
        long expectedLeader = 5L;

        GatewayServer gatewayServer = new GatewayServer(8888,9040,0,gateAwayID,new ConcurrentHashMap<>(otherPeers),1);
        servers = createServers(peerMap);
        startServers(servers);
        gatewayServer.start();
        Thread.sleep(20000);
        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader,server.getCurrentLeader().getProposedLeaderID());
        }
        System.out.println("KILLING LEADER");
        for(PeerServerImpl server : servers) {
            if(server.id == 5L  || server.id == 4L)
                server.shutdown();
        }
        Thread.sleep(70000);
        for(PeerServer server : servers) {
            if(server.getServerId() != 4L && server.getServerId() != 5L) {
                Assertions.assertEquals(3L, server.getCurrentLeader().getProposedLeaderID());
            }
        }
        gatewayServer.shutdown();
        gatewayServer.join(3000);
    }
    @Test
    public void testElectionAndKillLeaderAndSendWorkToNewLeader() throws Exception {
        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 6010));
        peerMap.put(2L, new InetSocketAddress("localhost", 6020));
        peerMap.put(3L, new InetSocketAddress("localhost", 6030));
        peerMap.put(4L, new InetSocketAddress("localhost", 6060));
        peerMap.put(5L, new InetSocketAddress("localhost", 6070)); //start 5.
        peerMap.put(gateAwayID, new InetSocketAddress("localhost", 6040));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(gateAwayID);
        long expectedLeader = 5L;

        GatewayServer gatewayServer = new GatewayServer(8888,6040,0,gateAwayID,new ConcurrentHashMap<>(otherPeers),1);
        servers = createServers(peerMap);
        startServers(servers);
        gatewayServer.start();
        Thread.sleep(18000);
        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader,server.getCurrentLeader().getProposedLeaderID());
        }
        System.out.println("KILLING LEADER");
        for(PeerServerImpl server : servers) {
            if(server.id == 5L  || server.id == 4L)
                server.shutdown();
        }
        Thread.sleep(70000);
        for(PeerServer server : servers) {
            if(server.getServerId() != 4L && server.getServerId() != 5L) {
                Assertions.assertEquals(3L, server.getCurrentLeader().getProposedLeaderID());
            }
        }
        System.out.println("GATEWAY LEADER: " + gatewayServer.gatewayPeerServer.currentLeader.getProposedLeaderID());
        ClientSim client = new ClientSim(gateAwayHost,gateAwayPort);
        System.out.println("SENDING WORK ");
        String res = client.sendWorkRequest(this.VALID_HELLO_WORLD);
        System.out.println("RES" + res);
        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, res);
        gatewayServer.shutdown();
        System.out.println("SHUTDOWN GATE AWAY ");
        for(PeerServer server : servers) {
            if(server.getServerId() != 4L && server.getServerId() != 5L)
                server.shutdown();

        }
    }
    @Test
    public void testElectionAndKillWorkerAndSendWorkToLeader() throws Exception {
        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 5010));
        peerMap.put(2L, new InetSocketAddress("localhost", 5020));
        peerMap.put(3L, new InetSocketAddress("localhost", 5030));
        peerMap.put(4L, new InetSocketAddress("localhost", 5060));
        peerMap.put(5L, new InetSocketAddress("localhost", 5070)); //start 5.
        peerMap.put(gateAwayID, new InetSocketAddress("localhost", 5040));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(gateAwayID);
        long expectedLeader = 5L;

        GatewayServer gatewayServer = new GatewayServer(8888,5040,0,gateAwayID,new ConcurrentHashMap<>(otherPeers),1);
        servers = createServers(peerMap);
        startServers(servers);
        gatewayServer.start();
        Thread.sleep(18000);
        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader,server.getCurrentLeader().getProposedLeaderID());
        }
        System.out.println("KILLING WORKER");
        for(PeerServerImpl server : servers) {
            if( server.id == 4L)
                server.shutdown();
        }
        Thread.sleep(10000);

        ClientSim client = new ClientSim(gateAwayHost,gateAwayPort);
        System.out.println("SENDING WORK ");
        String res = client.sendWorkRequest(this.VALID_HELLO_WORLD);
        System.out.println("RES" + res);
        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, res);
        gatewayServer.shutdown();
        System.out.println("SHUTDOWN GATE AWAY ");
        for(PeerServer server : servers) {
            if(server.getServerId() != 4L && server.getServerId() != 5L)
                server.shutdown();

        }

    }
    @Test
    public void testRealSIM() throws Exception {
        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 3010));
        peerMap.put(2L, new InetSocketAddress("localhost", 3020));
        peerMap.put(3L, new InetSocketAddress("localhost", 3030));
        peerMap.put(4L, new InetSocketAddress("localhost", 3060));
        peerMap.put(5L, new InetSocketAddress("localhost", 3070));
        peerMap.put(6L, new InetSocketAddress("localhost", 3080));
        peerMap.put(7L, new InetSocketAddress("localhost", 3090)); //start 5.
        peerMap.put(gateAwayID, new InetSocketAddress("localhost", 3040));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(gateAwayID);
        long expectedLeader = 7L;

        GatewayServer gatewayServer = new GatewayServer(8888, 3040, 0, gateAwayID, new ConcurrentHashMap<>(otherPeers), 1);
        servers = createServers(peerMap);
        startServers(servers);
        gatewayServer.start();
        Thread.sleep(10000);
        for (PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader, server.getCurrentLeader().getProposedLeaderID());
        }
        System.out.println("KILLING WORKER");
        for (PeerServerImpl server : servers) {
            if (server.id == 1L)
                server.shutdown();
        }
        Thread.sleep(10000);
        System.out.println("KILLING LEADER");
        for(PeerServerImpl server : servers) {
            if(server.id == 7L)
                server.shutdown();
        }
        Thread.sleep(1000);
        ClientSim client = new ClientSim(gateAwayHost, gateAwayPort);
        for(int i = 0; i < 9; i++){
            System.out.println("SENDING WORK ");
            String res = client.sendWorkRequest(this.VALID_HELLO_WORLD);
            System.out.println("RES" + res);
            Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, res);
        }

        gatewayServer.shutdown();
        gatewayServer.join(3000);

        for(PeerServer server : servers) {
            if(server.getServerId() != 1L && server.getServerId() != 7L) {
                server.shutdown();
            }
        }

        for(PeerServerImpl server : servers) {
            if(server.getServerId() != 1L && server.getServerId() != 7L) {
                server.join(5000);
            }
        }
    }

    private ArrayList<PeerServerImpl> createServers(HashMap<Long, InetSocketAddress> peerMap) throws IOException {
        ArrayList<PeerServerImpl> serverList = new ArrayList<>();
        for (Map.Entry<Long, InetSocketAddress> entry : peerMap.entrySet()) {
            if(entry.getKey() == gateAwayID)
                continue;
            HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
            otherPeers.remove(entry.getKey());
            PeerServerImpl server;
            server = new PeerServerImpl(
                    entry.getValue().getPort(),
                    0,
                    entry.getKey(),
                    otherPeers,
                    gateAwayID,
                    1
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