package com.rahul.kafkatimetravelengine.exception;

public class JobNotFoundException extends RuntimeException {
    public JobNotFoundException(String jobId) {
        super("No audit record found for jobId: " + jobId);
    }
}
