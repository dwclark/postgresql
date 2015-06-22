package db.postgresql.protocol.v3.io;

import java.nio.ByteBuffer;

public interface IO {
    void write(ByteBuffer toWrite);
    void read(ByteBuffer toRead);
}
