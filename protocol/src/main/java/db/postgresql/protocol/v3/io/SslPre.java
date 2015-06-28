package db.postgresql.protocol.v3.io;

import java.nio.channels.SocketChannel;

public interface SslPre {
    void pre(SocketChannel channel);
}
