package db.postgresql.protocol.v3;

import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.io.IOException;
import db.postgresql.protocol.v3.io.Stream;

public class KeyData extends Response {

    private final int processId;
    private final int key;

    public int getProcessId() {
        return processId;
    }

    public int getKey() {
        return key;
    }
    
    private KeyData(final BackEnd backEnd, final int processId, final int key) {
        super(backEnd);
        this.processId = processId;
        this.key = key;
    }
    
    public static final ResponseBuilder builder = new ResponseBuilder() {
            public KeyData build(final BackEnd backEnd, final int size, final Stream stream) {
                return new KeyData(backEnd, stream.getInt(), stream.getInt());
            }
        };
}
