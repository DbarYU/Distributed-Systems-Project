package edu.yu.cs.com3800.stage2;

import edu.yu.cs.com3800.PeerServer;
import edu.yu.cs.com3800.Vote;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class Stage2Test {

    private List<PeerServerImpl> servers;

    @BeforeEach
    public void setUp() {
        servers = new ArrayList<>();
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
    public void testElectionWith3Servers() throws Exception {

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 8010));
        peerMap.put(2L, new InetSocketAddress("localhost", 8020));
        peerMap.put(3L, new InetSocketAddress("localhost", 8030));

        servers = createServers(peerMap);
        startServers(servers);
        Thread.sleep(7000);
        for(PeerServer server : servers) {
            Assertions.assertEquals(3L,server.getCurrentLeader().getProposedLeaderID());
        }
    }
    @Test
    public void testElectionWith5Servers() throws Exception {

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 9010));
        peerMap.put(2L, new InetSocketAddress("localhost", 9020));
        peerMap.put(3L, new InetSocketAddress("localhost", 9030));
        peerMap.put(4L, new InetSocketAddress("localhost", 9040));
        peerMap.put(5L, new InetSocketAddress("localhost", 9050));

        servers = createServers(peerMap);
        startServers(servers);
        Thread.sleep(5000);
        for(PeerServer server : servers) {
            Assertions.assertEquals(5L,server.getCurrentLeader().getProposedLeaderID());
        }
    }
    @Test
    public void testElectionWith7Servers() throws Exception {

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 10010));
        peerMap.put(2L, new InetSocketAddress("localhost", 10020));
        peerMap.put(3L, new InetSocketAddress("localhost", 10030));
        peerMap.put(4L, new InetSocketAddress("localhost", 10040));
        peerMap.put(5L, new InetSocketAddress("localhost", 10050));
        peerMap.put(6L, new InetSocketAddress("localhost", 10060));
        peerMap.put(7L, new InetSocketAddress("localhost", 10070));

        servers = createServers(peerMap);
        startServers(servers);
        Thread.sleep(5000);
        for(PeerServer server : servers) {
            Assertions.assertEquals(7L,server.getCurrentLeader().getProposedLeaderID());
        }
    }
    @Test
    public void testElectionWith9Servers() throws Exception {

        HashMap<Long, InetSocketAddress> peerMap = new HashMap<>();
        peerMap.put(1L, new InetSocketAddress("localhost", 11010));
        peerMap.put(2L, new InetSocketAddress("localhost", 11020));
        peerMap.put(3L, new InetSocketAddress("localhost", 11030));
        peerMap.put(4L, new InetSocketAddress("localhost", 11040));
        peerMap.put(5L, new InetSocketAddress("localhost", 11050));
        peerMap.put(6L, new InetSocketAddress("localhost", 11060));
        peerMap.put(7L, new InetSocketAddress("localhost", 11070));
        peerMap.put(8L, new InetSocketAddress("localhost", 11080));
        peerMap.put(9L, new InetSocketAddress("localhost", 11090));

        servers = createServers(peerMap);
        startServers(servers);
        Thread.sleep(5000);
        for(PeerServer server : servers) {
            Assertions.assertEquals(9L,server.getCurrentLeader().getProposedLeaderID());
        }
    }
    @Test
    public void testElectionWith5ServersStaggeredStart() throws Exception {
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