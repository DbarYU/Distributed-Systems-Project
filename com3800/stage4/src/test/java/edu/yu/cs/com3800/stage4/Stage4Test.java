package edu.yu.cs.com3800.stage4;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import edu.yu.cs.com3800.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Stage4Test {


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
    @BeforeEach
    public void setupLoggingFolder() throws IOException {
        String runID = "run-" + System.currentTimeMillis();
        Globals.RUN_ID = runID;

        Path testDir = Paths.get("logs", runID);
        Files.createDirectories(testDir);
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



        GatewayServer gatewayServer = new GatewayServer(8888,8040,0,4L,new ConcurrentHashMap<>(otherPeers),1);
        gatewayServer.startPeerServer();


        servers = createServers(peerMap);
        startServers(servers);


        Thread.sleep(11000);
        gatewayServer.startHttpServer();

        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader,server.getCurrentLeader().getProposedLeaderID());
        }


        //after we asserted that this is the leader,
        //lets assign work from a client and assert that it has been completed.
        ClientSim client = new ClientSim(gateAwayHost,gateAwayPort);
        String res = client.sendWorkRequest(this.VALID_HELLO_WORLD);
        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, res);
        gatewayServer.shutdown();

        for(PeerServer server : servers) {
            server.shutdown();
        }
    }
    @Test
    public void testElectionAndGenerateMANYWorkWith3Servers() throws Exception {

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 8010));
        peerMap.put(2L, new InetSocketAddress("localhost", 8020));
        peerMap.put(3L, new InetSocketAddress("localhost", 8030));
        peerMap.put(gateAwayID, new InetSocketAddress("localhost", 8040));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(gateAwayID);
        long expectedLeader = 3L;

        GatewayServer gatewayServer = new GatewayServer(8888,8040,0,4L,new ConcurrentHashMap<>(otherPeers),1);

        gatewayServer.startPeerServer();
        servers = createServers(peerMap);
        startServers(servers);

        Thread.sleep(10000);
        gatewayServer.startHttpServer();

        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader,server.getCurrentLeader().getProposedLeaderID());
        }


        ClientSim client = new ClientSim(gateAwayHost,gateAwayPort);
        for (Map.Entry<String,String> entry : this.codeToResponseMap.entrySet()){
            String res =  client.sendWorkRequest(entry.getValue());
            Assertions.assertEquals(entry.getKey(), res);
        }
        gatewayServer.shutdown();

        for(PeerServer server : servers) {
            server.shutdown();
        }
    }

    @Test
    public void testElectionAndGenerateWorkWith3ServersCACHE() throws Exception {

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 5010));
        peerMap.put(2L, new InetSocketAddress("localhost", 4020));
        peerMap.put(3L, new InetSocketAddress("localhost", 5030));
        peerMap.put(gateAwayID, new InetSocketAddress("localhost", 5040));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(gateAwayID);
        long expectedLeader = 3L;

        GatewayServer gatewayServer = new GatewayServer(8888,5040,0,4L,new ConcurrentHashMap<>(otherPeers),1);
        gatewayServer.startPeerServer();

        servers = createServers(peerMap);
        startServers(servers);


        Thread.sleep(10000);
        gatewayServer.startHttpServer();

        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader,server.getCurrentLeader().getProposedLeaderID());
        }


        //after we asserted that this is the leader,
        //lets assign work from a client and assert that it has been completed.
        ClientSim client = new ClientSim(gateAwayHost,gateAwayPort);
        String res = client.sendWorkRequest(this.VALID_HELLO_WORLD);

        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, res);
        String res2 = client.sendWorkRequest(this.VALID_HELLO_WORLD);

        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, res2);
        Assertions.assertTrue(client.cached);

        gatewayServer.shutdown();

        for(PeerServer server : servers) {
            server.shutdown();
        }

    }
    @Test
    public void testElectionAndGenerateMANYWorkWith3ServersCACHE() throws Exception {

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 5010));
        peerMap.put(2L, new InetSocketAddress("localhost", 4020));
        peerMap.put(3L, new InetSocketAddress("localhost", 5030));
        peerMap.put(gateAwayID, new InetSocketAddress("localhost", 5040));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(gateAwayID);
        long expectedLeader = 3L;
        GatewayServer gatewayServer = new GatewayServer(8888,5040,0,4L,new ConcurrentHashMap<>(otherPeers),1);
        gatewayServer.startPeerServer();

        servers = createServers(peerMap);
        startServers(servers);


        Thread.sleep(10000);
        gatewayServer.startHttpServer();

        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader,server.getCurrentLeader().getProposedLeaderID());
        }


        //after we asserted that this is the leader,
        //lets assign work from a client and assert that it has been completed.
        ClientSim client = new ClientSim(gateAwayHost,gateAwayPort);

        for (int i =0; i < 30; i++){
            String res = client.sendWorkRequest(this.VALID_HELLO_WORLD);
            Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, res);
            if(i > 0)
                Assertions.assertTrue(client.cached);
        }

        gatewayServer.shutdown();

        for(PeerServer server : servers) {
            server.shutdown();
        }

    }
    @Test
    public void testElectionAndGenerateInvalidWorkWith3Servers() throws Exception {
        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 5010));
        peerMap.put(2L, new InetSocketAddress("localhost", 4020));
        peerMap.put(3L, new InetSocketAddress("localhost", 5030));
        peerMap.put(gateAwayID, new InetSocketAddress("localhost", 5040));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(gateAwayID);
        long expectedLeader = 3L;
        GatewayServer gatewayServer = new GatewayServer(8888,5040,0,4L,new ConcurrentHashMap<>(otherPeers),1);
        gatewayServer.startPeerServer();

        servers = createServers(peerMap);
        startServers(servers);


        Thread.sleep(10000);
        gatewayServer.startHttpServer();

        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader,server.getCurrentLeader().getProposedLeaderID());
        }


        //after we asserted that this is the leader,
        //lets assign work from a client and assert that it has been completed.
        ClientSim client = new ClientSim(gateAwayHost,gateAwayPort);
        String res = client.sendWorkRequest(this.INVALID_CODE);
        Assertions.assertEquals(INVALID_RESPONSE, res);
        gatewayServer.shutdown();

        for(PeerServer server : servers) {
            server.shutdown();
        }
    }
    @Test
    public void testElectionAndGenerateMANYInvalidWorkWith3Servers() throws Exception {
        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 5010));
        peerMap.put(2L, new InetSocketAddress("localhost", 4020));
        peerMap.put(3L, new InetSocketAddress("localhost", 5030));
        peerMap.put(gateAwayID, new InetSocketAddress("localhost", 5040));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(gateAwayID);
        long expectedLeader = 3L;
        GatewayServer gatewayServer = new GatewayServer(8888,5040,0,4L,new ConcurrentHashMap<>(otherPeers),1);
        gatewayServer.startPeerServer();

        servers = createServers(peerMap);
        startServers(servers);


        Thread.sleep(10000);
        gatewayServer.startHttpServer();

        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader,server.getCurrentLeader().getProposedLeaderID());
        }


        //after we asserted that this is the leader,
        //lets assign work from a client and assert that it has been completed.
        ClientSim client = new ClientSim(gateAwayHost,gateAwayPort);
        for(int i =0; i < 30; i++){
            String res = client.sendWorkRequest(this.INVALID_CODE);
            Assertions.assertEquals(INVALID_RESPONSE, res);
            if(i > 0)
                Assertions.assertTrue(client.cached);
        }

        gatewayServer.shutdown();

        for(PeerServer server : servers) {
            server.shutdown();
        }
    }
    @Test
    public void testElectionAndGenerateWorkWith12Servers() throws Exception {
        int basePort = 20000;
        int gateAwayUDPPort = 2040;
        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        for (int i = 1; i <= 11; i++) {
            peerMap.put((long)i, new InetSocketAddress("localhost", basePort + (i * 10)));
        }
        peerMap.put(this.gateAwayID, new InetSocketAddress("localhost", gateAwayUDPPort));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(this.gateAwayID);

        long expectedLeader = 11L;

        servers = createServers(peerMap);
        startServers(servers);

        GatewayServer gatewayServer = new GatewayServer(8888, gateAwayUDPPort, 0, this.gateAwayID, new ConcurrentHashMap<>(otherPeers), 1);
        gatewayServer.startPeerServer();

        Thread.sleep(12000);
        gatewayServer.startHttpServer();

        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader, server.getCurrentLeader().getProposedLeaderID());
        }

        ClientSim client = new ClientSim(gateAwayHost, gateAwayPort);
        String res = client.sendWorkRequest(this.VALID_HELLO_WORLD);
        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, res);
        gatewayServer.shutdown();

        for(PeerServer server : servers) {
            server.shutdown();
        }
    }

    @Test
    public void testElectionAndGenerateMANYWorkWith12Servers() throws Exception {
        int basePort = 30000;
        int gateAwayUDPPort = 2040;
        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        for (int i = 1; i <= 11; i++) {
            peerMap.put((long)i, new InetSocketAddress("localhost", basePort + (i * 10)));
        }
        peerMap.put(this.gateAwayID, new InetSocketAddress("localhost", gateAwayUDPPort));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(this.gateAwayID);

        long expectedLeader = 11L;

        servers = createServers(peerMap);
        startServers(servers);

        GatewayServer gatewayServer = new GatewayServer(8888, gateAwayUDPPort, 0, this.gateAwayID, new ConcurrentHashMap<>(otherPeers), 1);
        gatewayServer.startPeerServer();

        Thread.sleep(10000);
        gatewayServer.startHttpServer();

        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader, server.getCurrentLeader().getProposedLeaderID());
        }

        ClientSim client = new ClientSim(gateAwayHost, gateAwayPort);
        for (Map.Entry<String,String> entry : this.codeToResponseMap.entrySet()){
            String res =  client.sendWorkRequest(entry.getValue());
            Assertions.assertEquals(entry.getKey(), res);
        }
        gatewayServer.shutdown();

        for(PeerServer server : servers) {
            server.shutdown();
        }
    }

    @Test
    public void testElectionAndGenerateWorkWith12ServersCACHE() throws Exception {
        int basePort = 10000;
        int gateAwayUDPPort = 2040;
        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        for (int i = 1; i <= 11; i++) {
            peerMap.put((long)i, new InetSocketAddress("localhost", basePort + (i * 10)));
        }
        peerMap.put(this.gateAwayID, new InetSocketAddress("localhost", gateAwayUDPPort));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(this.gateAwayID);

        long expectedLeader = 11L;

        servers = createServers(peerMap);
        startServers(servers);

        GatewayServer gatewayServer = new GatewayServer(8888, gateAwayUDPPort, 0, this.gateAwayID, new ConcurrentHashMap<>(otherPeers), 1);
        gatewayServer.startPeerServer();

        Thread.sleep(10000);
        gatewayServer.startHttpServer();

        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader, server.getCurrentLeader().getProposedLeaderID());
        }

        ClientSim client = new ClientSim(gateAwayHost, gateAwayPort);
        String res = client.sendWorkRequest(this.VALID_HELLO_WORLD);

        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, res);
        String res2 = client.sendWorkRequest(this.VALID_HELLO_WORLD);

        Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, res2);
        Assertions.assertTrue(client.cached);

        gatewayServer.shutdown();

        for(PeerServer server : servers) {
            server.shutdown();
        }
    }

    @Test
    public void testElectionAndGenerateMANYWorkWith12ServersCACHE() throws Exception {
        int basePort = 13000;
        int gateAwayUDPPort = 2040;
        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        for (int i = 1; i <= 11; i++) {
            peerMap.put((long)i, new InetSocketAddress("localhost", basePort + (i * 10)));
        }
        peerMap.put(this.gateAwayID, new InetSocketAddress("localhost", gateAwayUDPPort));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(this.gateAwayID);

        long expectedLeader = 11L;

        servers = createServers(peerMap);
        startServers(servers);

        GatewayServer gatewayServer = new GatewayServer(8888, gateAwayUDPPort, 0, this.gateAwayID, new ConcurrentHashMap<>(otherPeers), 1);
        gatewayServer.startPeerServer();

        Thread.sleep(10000);
        gatewayServer.startHttpServer();

        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader, server.getCurrentLeader().getProposedLeaderID());
        }

        ClientSim client = new ClientSim(gateAwayHost, gateAwayPort);


        for (int i =0; i < 30; i++){
            String res = client.sendWorkRequest(this.VALID_HELLO_WORLD);
            Assertions.assertEquals(VALID_HELLO_WORLD_RESPONSE, res);
            if(i > 0)
                Assertions.assertTrue(client.cached);
        }

        gatewayServer.shutdown();

        for(PeerServer server : servers) {
            server.shutdown();
        }
    }

    @Test
    public void testElectionAndGenerateInvalidWorkWith12Servers() throws Exception {
        int basePort = 13000;
        int gateAwayUDPPort = 2040;
        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        for (int i = 1; i <= 11; i++) {
            peerMap.put((long)i, new InetSocketAddress("localhost", basePort + (i * 10)));
        }
        peerMap.put(this.gateAwayID, new InetSocketAddress("localhost", gateAwayUDPPort));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(this.gateAwayID);

        long expectedLeader = 11L;

        servers = createServers(peerMap);
        startServers(servers);

        GatewayServer gatewayServer = new GatewayServer(8888, gateAwayUDPPort, 0, this.gateAwayID, new ConcurrentHashMap<>(otherPeers), 1);
        gatewayServer.startPeerServer();

        Thread.sleep(10000);
        gatewayServer.startHttpServer();

        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader, server.getCurrentLeader().getProposedLeaderID());
        }

        ClientSim client = new ClientSim(gateAwayHost, gateAwayPort);
        String res = client.sendWorkRequest(this.INVALID_CODE);
        Assertions.assertEquals(INVALID_RESPONSE, res);
        gatewayServer.shutdown();

        for(PeerServer server : servers) {
            server.shutdown();
        }
    }

    @Test
    public void testElectionAndGenerateMANYInvalidWorkWith12Servers() throws Exception {
        int basePort = 15000;
        int gateAwayUDPPort = 2040;
        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        for (int i = 1; i <= 11; i++) {
            peerMap.put((long)i, new InetSocketAddress("localhost", basePort + (i * 10)));
        }
        peerMap.put(this.gateAwayID, new InetSocketAddress("localhost", gateAwayUDPPort));

        HashMap<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
        otherPeers.remove(this.gateAwayID);

        long expectedLeader = 11L;

        servers = createServers(peerMap);
        startServers(servers);

        GatewayServer gatewayServer = new GatewayServer(8888, gateAwayUDPPort, 0, this.gateAwayID, new ConcurrentHashMap<>(otherPeers), 1);
        gatewayServer.startPeerServer();

        Thread.sleep(10000);
        gatewayServer.startHttpServer();

        for(PeerServer server : servers) {
            Assertions.assertEquals(expectedLeader, server.getCurrentLeader().getProposedLeaderID());
        }

        ClientSim client = new ClientSim(gateAwayHost, gateAwayPort);
        for(int i =0; i < 30; i++){
            String res = client.sendWorkRequest(this.INVALID_CODE);
            Assertions.assertEquals(INVALID_RESPONSE, res);
            if(i > 0)
                Assertions.assertTrue(client.cached);
        }

        gatewayServer.shutdown();

        for(PeerServer server : servers) {
            server.shutdown();
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