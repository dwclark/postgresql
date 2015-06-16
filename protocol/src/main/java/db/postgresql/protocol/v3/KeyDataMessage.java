package db.postgresql.protocol.v3;

import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.io.IOException;

public class KeyDataMessage extends BackEndMessage {

    private final int processId;
    private final int key;

    public int getProcessId() {
        return processId;
    }

    public int getKey() {
        return key;
    }
    
    private KeyDataMessage(final BackEnd backEnd, final int processId, final int key) {
        super(backEnd);
        this.processId = processId;
        this.key = key;
    }
    
    public static final BackEndBuilder builder = new BackEndBuilder() {
            public KeyDataMessage read(final BackEnd backEnd, final int size, final Session session) {
                ByteBuffer payload = session.read(size);
                return new KeyDataMessage(backEnd, payload.getInt(), payload.getInt());
            }
        };
}
