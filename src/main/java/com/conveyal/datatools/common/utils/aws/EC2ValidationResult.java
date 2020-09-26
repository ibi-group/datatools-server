package com.conveyal.datatools.common.utils.aws;

/**
 * A helper class that returns a validation result and accompanying message.
 */
public class EC2ValidationResult {
    private Exception exception;

    private String message;

    private boolean valid = true;

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
        // add to list of supressed exceptions if needed
        if (taskValidationResult.exception != null) {
            if (this.exception == null) {
                throw new IllegalStateException("Must have initialized exception before appending");
            }
            this.exception.addSuppressed(taskValidationResult.exception);
        }
    }
}
