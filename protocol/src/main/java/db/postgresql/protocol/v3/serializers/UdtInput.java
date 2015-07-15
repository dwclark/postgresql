package db.postgresql.protocol.v3.serializers;

public interface UdtInput {
    short readShort();
    int readInt();
    long readLong();
    float readFloat();
    double readDouble();
}
