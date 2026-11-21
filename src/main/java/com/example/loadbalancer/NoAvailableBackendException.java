package com.example.loadbalancer;

/**
 * Thrown when every backend is marked unavailable.
 */
public final class NoAvailableBackendException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    public NoAvailableBackendException(String message) {
        super(message);
    }
}
