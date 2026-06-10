package edu.yu.cs.com3800.stage5;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.io.File;
import java.util.Arrays;
import java.util.Comparator;


public class RunDemo {
    static HttpClient client =  HttpClient.newHttpClient();
    public static void main(String[] args) throws IOException, InterruptedException {
        String nodes = waitUntilLeaderIsElected();
        System.out.println("NODES: " + nodes);
        String javaCode = """
            public class HelloWorld {
                public String run() {
                    return "Hello world!";
                }
            }
            """;

        for(int i = 0; i < 9; i++) {
            System.out.println("SENDING WORK: " + i);
            System.out.println("RESPONSE: " + sendClientRequest(javaCode));
        }

        Thread.sleep(1000);

        System.out.println("Step 3: Killing follower node 1...");
        int pid = getPidForPort(8010);
        killProcess(pid);

        System.out.println();
        System.out.println(waitUntilNode1IsDead());
        System.out.println("Step 6: Killing leader node 7...");
        int pid7 = getPidForPort(8070);

        killProcess(pid7);
        System.out.println("Node 7 (PID " + pid7 + ") has been killed with kill -9");
        System.out.println();
        System.out.println("SENDING MORE REQUESTS");
        for(int i = 0; i < 9; i++) {
            System.out.println("SENDING WORK: " + i);
            System.out.println("RESPONSE: " + sendClientRequest(javaCode));
        }

        Thread.sleep(35000);

        printCurrentLeader();
        for(int i = 0; i < 1; i++) {
            System.out.println("SENDING WORK: " + i);
            System.out.println("RESPONSE: " + sendClientRequest(javaCode));
        }
        printLogFilePaths();
        shutdownAllNodes();

    }
    private static String waitUntilLeaderIsElected() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/info"))
                .GET()
                .build();
        HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());
        if(response.body().trim().equals("NO LEADER SET")){
            Thread.sleep(2000);
            return waitUntilLeaderIsElected();
        }
        else
            return response.body();
    }

    private static String waitUntilNode1IsDead() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/info"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        String body = response.body();
        if (body.contains("\n1:") || body.startsWith("1:")) {
            Thread.sleep(2000);
            return waitUntilNode1IsDead();
        } else {
            System.out.println("Node 1 has been removed from the cluster!");
            return body;
        }
    }
    private static String sendClientRequest(String javaCode) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .POST(HttpRequest.BodyPublishers.ofString(javaCode))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }
    private static int getPidForPort(int port) {
        try {
            Process process = Runtime.getRuntime().exec(new String[]{"lsof", "-ti", ":" + port});
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = reader.readLine();
            if (line != null && !line.isEmpty()) {
                return Integer.parseInt(line.trim());
            }
        } catch (Exception e) {
            System.err.println("Error finding PID: " + e.getMessage());
        }
        return -1;
    }

    private static void killProcess(int pid) {
        try {
            Process process = Runtime.getRuntime().exec(new String[]{"kill", "-9", String.valueOf(pid)});
            process.waitFor();
        } catch (Exception e) {
            System.err.println("Error killing process: " + e.getMessage());
        }
    }
    private static Long getCurrentLeader() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8888/info"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        String body = response.body();
        String[] lines = body.split("\n");
        for (String line : lines) {
            if (line.contains(":leader")) {
                String[] parts = line.split(":");
                if (parts.length >= 2) {
                    try {
                        return Long.parseLong(parts[0].trim());
                    } catch (NumberFormatException ignored) {

                    }
                }
            }
        }
        return null;
    }

    private static void printCurrentLeader() throws IOException, InterruptedException {
        Long leaderID = getCurrentLeader();
        if (leaderID != null) {
            System.out.println("Current leader: Node " + leaderID);
        } else {
            System.out.println("No leader elected yet");
        }
    }

    private static void printLogFilePaths() {

        File logsBaseDir = new File(".");
        File[] logDirs = logsBaseDir.listFiles((dir, name) -> name.startsWith("logs-"));
        if (logDirs != null && logDirs.length > 0) {
            Arrays.sort(logDirs, Comparator.comparingLong(File::lastModified).reversed());
            File mostRecentLogDir = logDirs[0];
            System.out.println("Log directory: " + mostRecentLogDir.getAbsolutePath());
            System.out.println();
            int[] ports = {8010, 8020, 8030, 8040, 8050, 8060, 8070, 8080};
            int[] nodeIds = {1, 2, 3, 4, 5, 6, 7, 9999};
            for (int i = 0; i < nodeIds.length; i++) {
                int nodeId = nodeIds[i];
                int tcpPort = ports[i] + 2;
                System.out.println("Node " + nodeId + " (TCP Port " + tcpPort + "):");
                File[] nodeLogFiles = mostRecentLogDir.listFiles((dir, name) ->
                        name.contains("-on-" + nodeId + "-on-tcpPort" + tcpPort)
                );

                if (nodeLogFiles != null) {
                    for (File logFile : nodeLogFiles) {
                        if (logFile.getName().contains("VERBOSE")) {
                            System.out.println("  VERBOSE: " + logFile.getAbsolutePath());
                        } else if (logFile.getName().contains("SUMMARY")) {
                            System.out.println("  SUMMARY: " + logFile.getAbsolutePath());
                        } else if (logFile.getName().contains("REGULAR")) {
                            System.out.println("  REGULAR: " + logFile.getAbsolutePath());
                        }
                    }
                }
                System.out.println();
            }
        }
    }
    private static void shutdownAllNodes() {
        System.out.println();
        System.out.println("========================================");
        System.out.println("Shutting down all nodes...");
        System.out.println("========================================");
        try {
            Process killProcess = Runtime.getRuntime().exec(
                    new String[]{"pkill", "-9", "-f", "edu.yu.cs.com3800.stage5.StartServer"}
            );
            killProcess.waitFor();
            System.out.println("All server nodes have been shut down.");
        } catch (Exception e) {
            System.err.println("Error shutting down nodes: " + e.getMessage());

            System.out.println("Attempting to kill by port...");
            int[] ports = {8010, 8020, 8030, 8040, 8050, 8060, 8070, 8080, 8888};
            for (int port : ports) {
                try {
                    Process lsofProcess = Runtime.getRuntime().exec(
                            new String[]{"lsof", "-ti", ":" + port}
                    );
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(lsofProcess.getInputStream())
                    );
                    String pid = reader.readLine();
                    if (pid != null && !pid.isEmpty()) {
                        Runtime.getRuntime().exec(new String[]{"kill", "-9", pid}).waitFor();
                        System.out.println("Killed process on port " + port + " (PID: " + pid + ")");
                    }
                } catch (Exception ex) {
                }
            }
        }
        System.out.println("Shutdown complete.");
        System.out.println("========================================");
    }
}


