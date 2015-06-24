package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;

public interface ResponseBuilder {
    Response build(BackEnd backEnd, int size, Stream stream);
}
