package db.postgresql.protocol.v3.serializers;

public interface UdtInput {
    boolean hasNext();
    char getCurrentDelimiter();
    <T> T read(Class<T> type);
}
