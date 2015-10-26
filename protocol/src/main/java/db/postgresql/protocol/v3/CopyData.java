package db.postgresql.protocol.v3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.Charset;
import db.postgresql.protocol.v3.io.PostgresqlStream;

public class CopyData extends Response {

    private final PostgresqlStream stream;

    public CopyData(final PostgresqlStream stream, final int size) {
        super(BackEnd.CopyData, size);
        this.stream = stream;
    }

    public void toChannel(final WritableByteChannel channel) {
        try {
            int remaining = getSize();
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
}
