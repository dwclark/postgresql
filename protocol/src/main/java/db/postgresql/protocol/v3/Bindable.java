package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;

public interface Bindable {
    Format getFormat();
    int getLength();
    void write(Stream stream);
}
