package edu.yu.cs.com3800.stage1;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

public class ClientImpl implements Client {
    private final String hostName;
    private final int hostPort;
    private final HttpClient client;
    private final String baseUrl;
    private final java.net.http.HttpRequest.Builder builder;
    private CompletableFuture<Response> awaitingResponse;

    public ClientImpl(String hostName, int hostPort) throws MalformedURLException{
        this.hostName = hostName;
        this.hostPort = hostPort;
        this.client = HttpClient.newHttpClient();
        this.baseUrl = "http://" + hostName + ":" + hostPort;
        this. builder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/compileandrun"))
                .header("Content-Type", "text/x-java-source");
    }
    @Override
    public void sendCompileAndRunRequest(String src) throws IOException {
        HttpRequest request = this.builder.POST(HttpRequest.BodyPublishers.ofString(src))
                .build();
        awaitingResponse = client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(resp -> new Response(resp.statusCode(), resp.body()))
                .exceptionally(ex -> new Response(500, "Internal Server Error"));

    }

    @Override
    public Response getResponse() throws IOException {
        return this.awaitingResponse.join();
    }
}
