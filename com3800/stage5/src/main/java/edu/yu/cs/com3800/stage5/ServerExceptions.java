package edu.yu.cs.com3800.stage5;


public class ServerExceptions {


    public static class ServerException extends Exception {
        private final int statusCode;

        public ServerException(int statusCode, String message) {
            super(message);
            this.statusCode = statusCode;
        }

        public ServerException(int statusCode, String message, Throwable cause) {
            super(message, cause);
            this.statusCode = statusCode;
        }

        public int getStatusCode() {
            return statusCode;
        }

        @Override
        public String getMessage() {
            return super.getMessage();
        }
    }





    public static class InternalServerException extends ServerException {
        public InternalServerException(String message) {
            super(500, message);
        }

        public InternalServerException(String message, Throwable cause) {
            super(500, message, cause);
        }
    }




    public static class ConnectionException extends ServerException {
        public ConnectionException(String message) {
            super(502, message);
        }

        public ConnectionException(String message, Throwable cause) {
            super(502, message, cause);
        }

        public ConnectionException(String host, int port, Throwable cause) {
            super(502, "Failed to connect to " + host + ":" + port + " - " + cause.getMessage(), cause);
        }
    }








    public static class ExecutionException extends ServerException {
        public ExecutionException(String message) {
            super(422, message);
        }

    }


    public static class ServiceUnavailableException extends ServerException {
        public ServiceUnavailableException(String message) {
            super(503, message);
        }

        public ServiceUnavailableException(String message, Throwable cause) {
            super(503, message, cause);
        }
    }



}