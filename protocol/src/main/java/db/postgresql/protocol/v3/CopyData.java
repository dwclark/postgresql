package db.postgresql.protocol.v3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.Charset;

public class CopyData extends Response {

    private final int size;
    private final Session session;

    private CopyData(final Session session, final int size) {
        super(BackEnd.CopyData);
        this.session = session;
        this.size = size;
    }

    public void toChannel(final WritableByteChannel channel) {
        try {
            int remaining = size;
            while(remaining != 0) {
                ByteBuffer v = session.view(remaining);
                while(v.hasRemaining()) {
                    remaining -= channel.write(v);
                }

                if(remaining != 0) {
                    session.recv();
                }
            }
        }
        catch(IOException ex) {
            throw new ProtocolException(ex);
        }
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public CopyData build(final BackEnd backEnd, final int size, final Session session) {
                return new CopyData(session, size);
            }
        };
}
