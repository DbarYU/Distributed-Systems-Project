package edu.yu.cs.com3800.stage1;

import edu.yu.cs.com3800.*; 
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;

import java.nio.charset.StandardCharsets;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;

public class SimpleServerImpl implements SimpleServer {
    private static final Logger infoLogger = Logger.getLogger("InfoLogger");
    private static final Logger errorLogger = Logger.getLogger("ErrorLogger");

    static class Handler implements HttpHandler {
        /*
        This is our handler class, it responds to POST requests made to the path /compileandrun”
         */
        JavaRunner javaRunner;
        Handler(JavaRunner javaRunner) {
            this.javaRunner = javaRunner;
        }
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            if(!exchange.getRequestMethod().equals("POST")){
                errorLogger.severe("Method not supported");
                exchange.sendResponseHeaders(405,0);
                writeResponse(exchange,"error: Invalid HTTP method");
            }else if(contentType == null || !contentType.equals("text/x-java-source")){
                errorLogger.severe("Content type not supported");
                exchange.sendResponseHeaders(400,0);
                writeResponse(exchange,"error: Invalid Content-Type");
            }else {
                //valid request, lets process the data and leverage javaRunner so we can translate the string into executable code.
                String response;
                int rCode;
                String sourceCode = new String(exchange.getRequestBody().readAllBytes()).strip();
                infoLogger.info("Running SOURCE CODE: " + sourceCode);
                try {
                    response = javaRunner.compileAndRun(new ByteArrayInputStream(sourceCode.getBytes(StandardCharsets.UTF_8)));
                    infoLogger.info("Finished running java runner");
                    rCode = 200;
                } catch (ReflectiveOperationException | IllegalArgumentException | IOException e) {
                    ByteArrayOutputStream tmpStream = new ByteArrayOutputStream();
                    PrintStream ps = new PrintStream(tmpStream);
                    e.printStackTrace(ps);
                    String stackTrace = tmpStream.toString();

                    rCode = 400;
                    response = e.getMessage() +"\\n" + stackTrace;
                    errorLogger.severe("ERROR: "+e.getMessage());
                }catch (Exception e){
                    response = "error: "+e.getMessage();
                    rCode = 400;
                }

                exchange.sendResponseHeaders(rCode, response.getBytes(StandardCharsets.UTF_8).length);
                writeResponse(exchange, response);
            }
        }
        private void writeResponse(HttpExchange exchange,String response) throws IOException {
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }

    }
    int port;
    HttpServer server;


    public SimpleServerImpl(int port) throws IOException {
        initServer(port);
        setLoggers();

    }
    private void setLoggers() throws IOException {
        try {
            FileHandler fh = new FileHandler("server_logs.log", true);
            fh.setFormatter(new SimpleFormatter());
            infoLogger.addHandler(fh);
            errorLogger.addHandler(fh);
        } catch (Exception ignored) {

        }
    }
    private void initServer(int port) throws IOException {
        infoLogger.info("SimpleServer instantiated");
        this.port = port;
        //when I instantiate the server, i create a httpserver that can process HTTP requests
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/compileandrun", new Handler(new JavaRunner()));
    }

    public static void main(String[] args)
    {
        int port = 9000;
        if(args.length >0)
        {
            port = Integer.parseInt(args[0]);
        }
        SimpleServer myserver = null;
        try
        {
            myserver = new SimpleServerImpl(port);
            myserver.start();
        }
        catch(Exception e)
        {
            System.err.println(e.getMessage());
            myserver.stop();
        }
    }

    @Override
    public void start() {
        infoLogger.info("Http Server started");
        this.server.start();
    }

    @Override
    public void stop() {
        infoLogger.info("Http Server ended");
        this.server.stop(0);
    }
}
