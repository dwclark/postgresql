package db.postgresql.protocol.v3.types;

import db.postgresql.protocol.v3.serializers.UdtOutput;

public interface Udt {
    String getName();
    void write(UdtOutput output);

    default char getLeftDelimiter() {
        return '(';
    }

    default char getRightDelimiter() {
        return ')';
    }
}
