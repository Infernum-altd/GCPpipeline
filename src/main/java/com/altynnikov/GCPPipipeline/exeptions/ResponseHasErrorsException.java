package com.altynnikov.GCPPipipeline.exeptions;

public class ResponseHasErrorsException extends Exception {
    public ResponseHasErrorsException(String errorMessage) {
        super(errorMessage);
    }
}
