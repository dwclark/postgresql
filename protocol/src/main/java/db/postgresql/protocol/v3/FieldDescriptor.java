package db.postgresql.protocol.v3;

import java.nio.ByteBuffer;

public class FieldDescriptor {
    final String name;
    final int tableOid;
    final short columnOid;
    final int typeOid;
    final short typeSize;
    final int typeModifier;
    final Format format;

    private FieldDescriptor(final String name, final int tableOid, final short columnOid,
                           final int typeOid, final short typeSize, final int typeModifier, final Format format) {
        this.name = name;
        this.tableOid = tableOid;
        this.columnOid = columnOid;
        this.typeOid = typeOid;
        this.typeSize = typeSize;
        this.typeModifier = typeModifier;
        this.format = format;
    }

    public static FieldDescriptor from(Response r) {
        ByteBuffer b = r.getBuffer();
        return new FieldDescriptor(r.nullString(), b.getInt(), b.getShort(),
                                   b.getInt(), b.getShort(), b.getInt(), Format.from(b.getShort() & 0xFFFF));
    }
}
