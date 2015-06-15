package db.postgresql.protocol.v3;

import java.io.IOException;

public class ProtocolException extends RuntimeException {

    public ProtocolException() {
        super();
    }

    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(Throwable cause) {
        super(cause);
    }

    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
    }

    public static void from(IOException ioe) {
        throw new ProtocolException(ioe);
    }
}
