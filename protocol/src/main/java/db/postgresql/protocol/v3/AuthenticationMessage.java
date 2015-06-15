package db.postgresql.protocol.v3;

import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.io.IOException;

public class AuthenticationMessage extends BackEndMessage {
    
    private AuthenticationMessage(BackEnd backEnd) {
        super(backEnd);
    }

    public static AuthenticationMessage read(final BackEnd backEnd, final int size,
                                             final ScatteringByteChannel channel) throws IOException {
        ByteBuffer buffer = read(size, channel);
        return new AuthenticationMessage(BackEnd.find(backEnd.id, (byte) buffer.getInt(0)));
    }
}
