package db.postgresql.protocol.v3.serializers;

public interface UdtInput {
    boolean hasNext();
    char getCurrentDelimiter();
    Boolean readBoolean();
    Short readShort();
    Integer readInteger();
    Long readLong();
    Float readFloat();
    Double readDouble();
    <T extends Udt> T readUdt(Class<T> type);
}
