package db.postgresql.protocol.v3;

import java.nio.channels.ScatteringByteChannel;

public interface BackEndBuilder {
    BackEndMessage read(BackEnd backEnd, int size, Session session);
}
