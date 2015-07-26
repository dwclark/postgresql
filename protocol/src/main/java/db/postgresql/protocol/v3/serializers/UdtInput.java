package db.postgresql.protocol.v3.serializers;

public interface UdtInput {
    boolean readBoolean();
    short readShort();
    int readInt();
    long readLong();
    float readFloat();
    double readDouble();
}
