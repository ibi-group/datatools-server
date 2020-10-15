package com.conveyal.datatools.common.utils.aws;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A helper class that returns a validation result and accompanying message.
 */
public class EC2ValidationResult {
    private Exception exception;

    private String message;

    private boolean valid = true;

    public static EC2ValidationResult executeValidationTasks(
        List<Callable<EC2ValidationResult>> validationTasks, String overallInvalidMessage
    ) throws ExecutionException, InterruptedException {
        // create overall result
        EC2ValidationResult result = new EC2ValidationResult();

        // Create a thread pool that is the size of the total number of validation tasks so each task gets its own
        // thread
        ExecutorService pool = Executors.newFixedThreadPool(validationTasks.size());

        // Execute all tasks
        for (Future<EC2ValidationResult> resultFuture : pool.invokeAll(validationTasks)) {
            EC2ValidationResult taskResult = resultFuture.get();
            // check if task yielded a valid result
            if (!taskResult.isValid()) {
                // task had an invalid result, check if overall validation result has been changed to false yet
                if (result.isValid()) {
                    // first invalid result. Write a header message.
                    result.setInvalid(overallInvalidMessage);
                }
                // add to list of messages and exceptions
                result.appendResult(taskResult);
            }
        }
        pool.shutdown();
        return result;
    }

    public Exception getException() {
        return exception;
    }

    public String getMessage() {
        return message;
    }

    public boolean isValid() {
        return valid;
    }

    public void setInvalid(String message) {
        this.setInvalid(message, null);
    }

    public void setInvalid(String message, Exception e) {
        this.exception = e;
        this.message = message;
        this.valid = false;
    }

    public void appendResult(EC2ValidationResult taskValidationResult) {
        if (this.message == null)
            throw new IllegalStateException("Must have initialized message before appending");
        this.message = String.format("%s  - %s\n", this.message, taskValidationResult.message);
        // add to list of suppressed exceptions if needed
        if (taskValidationResult.exception != null) {
            if (this.exception == null) {
                throw new IllegalStateException("Must have initialized exception before appending");
            }
            this.exception.addSuppressed(taskValidationResult.exception);
        }
    }
}
