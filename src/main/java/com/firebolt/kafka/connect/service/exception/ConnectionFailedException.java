package com.firebolt.kafka.connect.service.exception;

/**
 * Exception thrown when a connection to Firebolt database fails.
 * This exception wraps the underlying cause and provides meaningful error messages
 * for connection-related failures.
 */
public class ConnectionFailedException extends RuntimeException {

    /**
     * Constructs a new ConnectionFailedException with the specified detail message.
     *
     * @param message the detail message explaining why the connection failed
     */
    public ConnectionFailedException(String message) {
        super(message);
    }

    /**
     * Constructs a new ConnectionFailedException with the specified detail message and cause.
     *
     * @param message the detail message explaining why the connection failed
     * @param cause the underlying cause of the connection failure
     */
    public ConnectionFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new ConnectionFailedException with the specified cause.
     * The detail message is derived from the cause.
     *
     * @param cause the underlying cause of the connection failure
     */
    public ConnectionFailedException(Throwable cause) {
        super("Connection to Firebolt failed", cause);
    }
}