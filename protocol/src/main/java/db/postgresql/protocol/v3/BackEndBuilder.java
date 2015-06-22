package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;

public interface BackEndBuilder {
    BackEndMessage read(BackEnd backEnd, int size, Stream stream);
}
