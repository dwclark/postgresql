package db.postgresql.protocol.v3;

import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import db.postgresql.protocol.v3.io.Stream;

public class CopyData extends Response {

    private final int size;
    private Stream stream;

    public CopyData(final BackEnd backEnd, final int size, final String stream) {
        super(backEnd);
        this.size = size;
        this.stream = stream;
    }

    public void toChannel(WritableByteChannel channel) {
        int left = size;
        left -= channel.write(stream.getRecvBuffer());
        while(left != 0) {
            stream.recv();
            left -= channel.write(stream.getRecvBuffer());
        }

        stream = null;
    }
    
    public static final ResponseBuilder builder = new ResponseBuilder() {
            public CopyData build(BackEnd backEnd, int size, Stream stream) {
                return new CopyData(backEnd, size, stream);
            }
        };
}
