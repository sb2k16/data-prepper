package org.opensearch.dataprepper.pipeline.exceptions;

public class InvalidEventHandleException extends RuntimeException {
    public InvalidEventHandleException(final String message) {
        super(message);
    }
}
