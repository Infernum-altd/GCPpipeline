package com.altynnikov.GCPPipipeline.exeptions;

public class AvroNoClientFoundException extends Exception {
    public AvroNoClientFoundException(String errorMessage) {
        super(errorMessage);
    }
}
