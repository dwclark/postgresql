package db.postgresql.protocol.v3;

import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.io.IOException;

public class BackEndFormatter extends Formatter {

    public static String fromNullString(ByteBuffer buffer, int size) {
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        if(bytes[bytes.length-1] == NULL) {
            return new String(bytes, 0, bytes.length - 1, charset);
        }
        else {
            return new String(bytes, charset);
        }
    }

    public static ByteBuffer read(final int toRead, final ScatteringByteChannel channel) {
        try {
            final ByteBuffer buffer = ByteBuffer.allocate(toRead);
            long read = 0;
            while(read != toRead) {
                read += channel.read(buffer);
            }
            
            return buffer;
        }
        catch(IOException ioe) {
            throw new ProtocolException(ioe);
        }
    }
}
