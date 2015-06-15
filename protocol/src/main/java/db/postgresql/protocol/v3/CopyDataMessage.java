package db.postgresql.protocol.v3;

import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class CopyDataMessage extends BackEndMessage {

    private final ByteBuffer data;

    public ByteBuffer getData() {
        return data;
    }
    
    public CopyDataMessage(final BackEnd backEnd, final ByteBuffer data) {
        super(backEnd);
        this.data = data;
    }

    public static final BackEndBuilder builder = new BackEndBuilder() {
            public CopyDataMessage read(BackEnd backEnd, int size, ScatteringByteChannel channel) {
                return new CopyDataMessage(backEnd, BackEndFormatter.read(size, channel));
            }
        };
}
