package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;

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

    public FieldDescriptor(final Stream stream) {
        this(stream.nullString(), stream.getInt(), stream.getShort(),
             stream.getInt(), stream.getShort(), stream.getInt(),
             Format.from(stream.getShort() & 0xFFFF));
    }

    @Override
    public int hashCode() {
        return (name.hashCode() + tableOid + columnOid + typeOid + typeSize + typeModifier + format.ordinal());
    }

    @Override
    public boolean equals(Object rhs) {
        if(!(rhs instanceof FieldDescriptor)) {
            return false;
        }

        FieldDescriptor fd = (FieldDescriptor) rhs;
        return (name.equals(fd.name) &&
                (tableOid == fd.tableOid) &&
                (columnOid == fd.columnOid) &&
                (typeOid == fd.typeOid) &&
                (typeSize == fd.typeSize) &&
                (typeModifier == fd.typeModifier) &&
                (format == fd.format));
    }
}
