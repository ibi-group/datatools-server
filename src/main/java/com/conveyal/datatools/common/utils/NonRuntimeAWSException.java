package com.conveyal.datatools.common.utils;

import com.amazonaws.services.ec2.model.AmazonEC2Exception;

/**
 * A helper exception class that does not extend the RunTimeException class in order to make the compiler properly
 * detect possible places where an exception could occur.
 */
public class NonRuntimeAWSException extends Exception {
    public final Exception originalException;

    public NonRuntimeAWSException(String message) {
        super(message);
        originalException = null;
    }

    public NonRuntimeAWSException(AmazonEC2Exception e) {
        super(e.getMessage());
        originalException = e;
    }
}
