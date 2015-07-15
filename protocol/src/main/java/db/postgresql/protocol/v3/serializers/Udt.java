package db.postgresql.protocol.v3.serializers;

public interface Udt {
    String getName();
    void read(UdtInput input);
    void write(UdtOutput output);
}
