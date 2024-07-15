package org.nashtech.com.exceptions;

public class InvalidJsonSchemaException extends RuntimeException {
    public InvalidJsonSchemaException(String message) {
        super(message);
    }
}