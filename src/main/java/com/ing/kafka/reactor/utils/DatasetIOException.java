package com.ing.kafka.reactor.utils;

import java.io.IOException;

/**
 * Created by helios on 11/04/19.
 */
public class DatasetIOException extends DatasetException {

    private final IOException ioException;

    public DatasetIOException(String message, IOException root) {
        super(message, root);
        this.ioException = root;
    }

    public IOException getIOException() {
        return ioException;
    }
}
