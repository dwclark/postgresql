package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.Charset;

public class CopyData extends Response {

    private final int size;
    private final Stream stream;

    private CopyData(final Stream stream, final int size) {
        super(BackEnd.CopyData);
        this.stream = stream;
        this.size = size;
    }

    public void toChannel(final WritableByteChannel channel) {
        try {
            int remaining = size;
            while(remaining != 0) {
                ByteBuffer v = stream.view(remaining);
                while(v.hasRemaining()) {
                    remaining -= channel.write(v);
                }

                if(remaining != 0) {
                    stream.recv();
                }
            }
        }
        catch(IOException ex) {
            throw new ProtocolException(ex);
        }
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public CopyData build(final BackEnd backEnd, final int size, final Stream stream) {
                return new CopyData(stream, size);
            }
        };
}
