package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StartServer{
    public static void main(String[] args) throws IOException {
        int index = Integer.parseInt(args[0]);
        Map<Long, InetSocketAddress> peerMap = new HashMap<>();
        int thisPort = -1;
        long thisID = -1;
        boolean isGateway = args[8].equals("G");
        String gatewayArg = args[9];
        int gatewayPort = Integer.parseInt(gatewayArg.split(":")[1]);
        long gatewayID = Long.parseLong(gatewayArg.split(":")[0]);
        for(int i = 1; i < 8; i++){
            String arg = args[i];
            int port = Integer.parseInt(arg.split(":")[1]);
            long id = Long.parseLong(arg.split(":")[0]);
            if(index == i ){
                thisPort = port;
                thisID = id;
            }
            peerMap.put(id, new InetSocketAddress("localhost", port));
        }

        peerMap.put(gatewayID, new InetSocketAddress("localhost", gatewayPort));

        Thread server;
        if(isGateway) {
            Map<Long, InetSocketAddress> otherPeers = new HashMap<>(peerMap);
            otherPeers.remove(gatewayID);
            server = new GatewayServer(8888, gatewayPort, 0, gatewayID, new ConcurrentHashMap<>(otherPeers), 1);
        } else {
            server = new PeerServerImpl(thisPort, 0, thisID, peerMap, gatewayID, 1);
        }
        server.start();
    }
}
