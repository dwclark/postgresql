package db.postgresql.protocol.v3.io;

import java.nio.ByteBuffer;

public abstract class IO {

    private final String host;
    private final int port;

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
    
    public IO(final String host, final int port) {
        this.host = host;
        this.port = port;
    }

    public abstract int getAppMinBufferSize();
    public abstract void write(ByteBuffer toWrite);
    public abstract void read(ByteBuffer toRead);
    public abstract void close();
    public abstract void wakeup();
}
