package edu.yu.cs.com3800.stage4;

/**
 * Container class for all server-related exceptions with HTTP status codes
 */
public class ServerExceptions {

    /**
     * Base exception class for server-related errors with HTTP status codes
     */
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

    /**
     * Exception for HTTP method not allowed errors (405)
     */
    public static class MethodNotAllowedException extends ServerException {
        public MethodNotAllowedException(String method) {
            super(405, "Method not supported: " + method);
        }
    }

    /**
     * Exception for unsupported content type errors (400)
     */
    public static class UnsupportedContentTypeException extends ServerException {
        public UnsupportedContentTypeException(String contentType) {
            super(400, "Content-Type not supported: " + (contentType == null ? "null" : contentType));
        }
    }

    /**
     * Exception for bad request errors (400)
     */
    public static class BadRequestException extends ServerException {
        public BadRequestException(String message) {
            super(400, message);
        }

        public BadRequestException(String message, Throwable cause) {
            super(400, message, cause);
        }
    }

    /**
     * Exception for internal server errors (500)
     */
    public static class InternalServerException extends ServerException {
        public InternalServerException(String message) {
            super(500, message);
        }

        public InternalServerException(String message, Throwable cause) {
            super(500, message, cause);
        }
    }

    /**
     * Exception for leader-related errors (503 Service Unavailable)
     */
    public static class LeaderNotAvailableException extends ServerException {
        public LeaderNotAvailableException(String message) {
            super(503, message);
        }

        public LeaderNotAvailableException() {
            super(503, "Leader has not been elected yet");
        }
    }

    /**
     * Exception for TCP connection errors (502 Bad Gateway)
     */
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

    /**
     * Exception for request parsing errors (400)
     */
    public static class RequestParseException extends ServerException {
        public RequestParseException(String message) {
            super(400, message);
        }

        public RequestParseException(String message, Throwable cause) {
            super(400, message, cause);
        }
    }

    /**
     * Exception for compilation errors (422 Unprocessable Entity)
     */
    public static class CompilationException extends ServerException {
        public CompilationException(String message) {
            super(422, message);
        }

        public CompilationException(String message, Throwable cause) {
            super(422, message, cause);
        }
    }

    /**
     * Exception for runtime execution errors (422)
     */
    public static class ExecutionException extends ServerException {
        public ExecutionException(String message) {
            super(422, message);
        }

        public ExecutionException(String message, Throwable cause) {
            super(422, message, cause);
        }
    }

    /**
     * Exception for service unavailable errors (503)
     */
    public static class ServiceUnavailableException extends ServerException {
        public ServiceUnavailableException(String message) {
            super(503, message);
        }

        public ServiceUnavailableException(String message, Throwable cause) {
            super(503, message, cause);
        }
    }

    /**
     * Exception for gateway timeout errors (504)
     */
    public static class GatewayTimeoutException extends ServerException {
        public GatewayTimeoutException(String message) {
            super(504, message);
        }

        public GatewayTimeoutException(String message, Throwable cause) {
            super(504, message, cause);
        }
    }
}