package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;

public class KeyData extends Response {

    private final int pid;
    private final int secretKey;

    public int getPid() {
        return pid;
    }

    public int getSecretKey() {
        return secretKey;
    }
    
    private KeyData(final BackEnd backEnd, final int pid, final int secretKey) {
        super(backEnd);
        this.pid = pid;
        this.secretKey = secretKey;
    }
    
    public static final ResponseBuilder builder = new ResponseBuilder() {
            public KeyData build(final BackEnd backEnd, final int size, final Stream stream) {
                return new KeyData(backEnd, stream.getInt(), stream.getInt());
            }
        };
}
