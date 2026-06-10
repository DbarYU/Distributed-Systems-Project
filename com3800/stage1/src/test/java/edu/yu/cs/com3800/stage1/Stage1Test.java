package edu.yu.cs.com3800.stage1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.yu.cs.com3800.stage1.SimpleServerImpl;
import edu.yu.cs.com3800.stage1.Client;
import edu.yu.cs.com3800.stage1.ClientImpl;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Unit test for simple App.
 */
public class Stage1Test {

    /**
     * Rigorous Test :-)
     */
    int port = 9000;
    private static ExecutorService executor;
    private static SimpleServerImpl server;
    private static Map<Integer,String> expectedToSource;

    @BeforeAll
    static void  runServer(){
        setupServer();
        createMaps();
    }
    static void setupServer(){
        executor = Executors.newSingleThreadExecutor();
        try {
            server = new SimpleServerImpl(9000);
        } catch (IOException e) {
            System.out.println("ERROR 1");
            throw new RuntimeException(e);
        }


        executor.submit(() -> {
            server.start();  
        });


        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            System.out.println("ERROR 2");
            throw new RuntimeException(e);
        }
    }
    static void createMaps(){
        expectedToSource = new HashMap<>();
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
            expectedToSource.put(i,code);
        }

    }
    @Test
    public void basicTestSuccess() {
        String validSourceCode = expectedToSource.get(1);
        String expectedResult = "1";

        try {
            Client client = new ClientImpl("localhost",port);
            client.sendCompileAndRunRequest(validSourceCode);
            Client.Response response = client.getResponse();
            printAndTestHelperSuccess(expectedResult,response.getBody());
            assertEquals(200,response.getCode());
        }catch (Exception e){
            System.out.println("ERROR:" + e);
            throw new AssertionError();
        }
    }
    @Test
    public void basicTestFailure() {
        String invalidSourceCode = "BLAH BLAH BLAH BLAH";
        try {
            Client client = new ClientImpl("localhost",port);
            client.sendCompileAndRunRequest(invalidSourceCode);
            Client.Response response = client.getResponse();
            printAndTestHelperFailure(response.getBody());
            assertEquals(400, response.getCode());
        }catch (Exception e) {
            System.out.println("ERROR:" + e);
            throw new AssertionError();

        }
    }
    @Test
    public void complexTestSuccessV1() {
        String invalidSourceCode = "";
        try {
            Client client = new ClientImpl("localhost",port);
            client.sendCompileAndRunRequest(invalidSourceCode);
            Client.Response response = client.getResponse();
            printAndTestHelperFailure(response.getBody());
            assertEquals(400, response.getCode());
        }catch (Exception e) {
            System.out.println("ERROR:" + e);
            throw new AssertionError();

        }
    }
    @Test
    public void complexTestSuccessV2() throws MalformedURLException {
        Client client = new ClientImpl("localhost", port);
        for (int val : expectedToSource.keySet()) {
            String validSourceCode = expectedToSource.get(val);
            String expectedResult = String.valueOf(val);
            try {
                client.sendCompileAndRunRequest(validSourceCode);
                Client.Response response = client.getResponse();
                printAndTestHelperSuccess(expectedResult, response.getBody());
                assertEquals(200, response.getCode());
            } catch (Exception e) {
                System.out.println("ERROR:" + e);
                throw new AssertionError();
            }
        }
    }
    @Test
    public void complexTestFailV1() {
        String invalidSourceCode = """
                public class TestRunner {
                    public TestRunner() {
                    }

                    public String rn() {
                        return "%d";
                    }
                }""";
        try {
            Client client = new ClientImpl("localhost",port);
            client.sendCompileAndRunRequest(invalidSourceCode);
            Client.Response response = client.getResponse();
            printAndTestHelperFailure(response.getBody());
            assertEquals(400, response.getCode());
        }catch (Exception e) {
            System.out.println("ERROR:" + e);
            throw new AssertionError();

        }
    }
    private void printAndTestHelperSuccess(String expected, String result){
        System.out.println("Expected response:");
        System.out.println(expected);
        System.out.println("Actual response:");
        System.out.println(result);
        assertEquals(expected, result, "Responses do not match");
    }
    private void printAndTestHelperFailure(String result){
        System.out.println("Expected response:");
        System.out.println("Failure...");
        System.out.println("Actual response:");
        System.out.println(result);
    }
}