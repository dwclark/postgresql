package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

public class CopyData extends Response {

    private final int size;
    private final Stream stream;

    public CopyData(final BackEnd backEnd, final int size, final Stream stream) {
        super(backEnd);
        this.size = size;
        this.stream = stream;
    }

    public void toChannel(WritableByteChannel channel) {
        try {
            int left = size;
            left -= channel.write(stream.getRecvBuffer());
            while(left != 0) {
                stream.recv();
                left -= channel.write(stream.getRecvBuffer());
            }
        }
        catch(IOException ex) {
            throw new ProtocolException(ex);
        }
    }
    
    public static final ResponseBuilder builder = new ResponseBuilder() {
            public CopyData build(final BackEnd backEnd, final int size, final Stream stream) {
                return new CopyData(backEnd, size, stream);
            }
        };
}
