package db.postgresql.protocol.v3.serializers;

public interface UdtOutput {
    void writeBoolean(boolean val);
    void writeShort(short val);
    void writeInt(int val);
    void writeLong(long val);
    void writeFloat(float val);
    void writeDouble(double val);
    void writeUdt(Udt udt);
}
