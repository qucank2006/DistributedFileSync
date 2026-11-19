package com.dacs4.filesync.shared.exception;

import java.io.IOException;

public class ProtocolException extends IOException {
    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
