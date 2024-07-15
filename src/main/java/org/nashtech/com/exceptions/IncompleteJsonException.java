package org.nashtech.com.exceptions;

public class IncompleteJsonException extends RuntimeException {
    public IncompleteJsonException(String message) {
        super(message);
    }
}