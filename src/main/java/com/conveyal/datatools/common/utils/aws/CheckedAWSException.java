package com.conveyal.datatools.common.utils.aws;

import com.amazonaws.AmazonServiceException;

/**
 * A helper exception class that does not extend the RunTimeException class in order to make the compiler properly
 * detect possible places where an exception could occur.
 */
public class CheckedAWSException extends Exception {
    public final Exception originalException;

    public CheckedAWSException(String message) {
        super(message);
        originalException = null;
    }

    public CheckedAWSException(AmazonServiceException e) {
        super(e.getMessage());
        originalException = e;
    }
}
