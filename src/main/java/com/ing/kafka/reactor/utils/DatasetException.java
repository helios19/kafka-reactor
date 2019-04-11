package com.ing.kafka.reactor.utils;

/**
 * Created by helios on 11/04/19.
 */
public class DatasetException extends RuntimeException {

    public DatasetException() {
        super();
    }

    public DatasetException(String message) {
        super(message);
    }

    public DatasetException(String message, Throwable t) {
        super(message, t);
    }

    public DatasetException(Throwable t) {
        super(t);
    }

    protected static String format(String message, Object... args) {
        String[] argStrings = new String[args.length];
        for (int i = 0; i < args.length; i += 1) {
            argStrings[i] = String.valueOf(args[i]);
        }
        return String.format(String.valueOf(message), (Object[]) argStrings);
    }
}