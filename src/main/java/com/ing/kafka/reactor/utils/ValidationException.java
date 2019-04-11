package com.ing.kafka.reactor.utils;

/**
 * Created by helios on 11/04/19.
 */
public class ValidationException extends DatasetException {

    public ValidationException(String msg) {
        super(msg);
    }

    public ValidationException(Throwable cause) {
        super(cause);
    }

    public ValidationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Precondition-style validation that throws a {@link ValidationException}.
     *
     * @param isValid
     *          {@code true} if valid, {@code false} if an exception should be
     *          thrown
     * @param message
     *          A String message for the exception.
     */
    public static void check(boolean isValid, String message, Object... args) {
        if (!isValid) {
            String[] argStrings = new String[args.length];
            for (int i = 0; i < args.length; i += 1) {
                argStrings[i] = String.valueOf(args[i]);
            }
            throw new ValidationException(
                    String.format(String.valueOf(message), (Object[]) argStrings));
        }
    }
}