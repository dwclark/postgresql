package db.postgresql.protocol.v3.serializers;

public interface UdtInput {
    boolean hasNext();
    char getCurrentDelimiter();
    boolean readBoolean();
    short readShort();
    int readInt();
    long readLong();
    float readFloat();
    double readDouble();
    <T extends Udt> T readUdt(Class<T> type);
}
