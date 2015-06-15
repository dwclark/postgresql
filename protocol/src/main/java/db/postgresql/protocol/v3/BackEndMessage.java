package db.postgresql.protocol.v3;

import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class BackEndMessage {

    public static final int HEADER_SIZE = 5;

    public BackEnd getBackEnd() {
        return backEnd;
    }

    private final BackEnd backEnd;

    protected BackEndMessage(final BackEnd backEnd) {
        this.backEnd = backEnd;
    }

    public static BackEndMessage read(final ScatteringByteChannel channel) throws IOException {
        ByteBuffer header = read(HEADER_SIZE, channel);
        BackEnd backEnd = BackEnd.find(header.get(0));
        int size = header.getInt(1) - 4; //subtract the size of the size field

        switch(backEnd) {
        case Authentication: return AuthenticationMessage.read(backEnd, size, channel);
        case BackendKeyData: return KeyDataMessage.read(backEnd, size, channel);
        case BindComplete: return new BackEndMessage(backEnd);
        case CloseComplete: return new BackEndMessage(backEnd);
        default: throw new UnsupportedOperationException();
        }
    }

    public static ByteBuffer read(final int toRead, final ScatteringByteChannel channel) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(toRead);
        long read = 0;
        while(read != toRead) {
            read += channel.read(buffer);
        }

        return buffer;
    }
}
