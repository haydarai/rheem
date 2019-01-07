package org.qcri.rheem.experiment;

public class ExperimentException extends RuntimeException {
    public ExperimentException() {
        super();
    }

    public ExperimentException(String message) {
        super(message);
    }

    public ExperimentException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExperimentException(Throwable cause) {
        super(cause);
    }

    protected ExperimentException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
