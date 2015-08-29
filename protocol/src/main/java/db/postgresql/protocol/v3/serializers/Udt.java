package db.postgresql.protocol.v3.serializers;

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
