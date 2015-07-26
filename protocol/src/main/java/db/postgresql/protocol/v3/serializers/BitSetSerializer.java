package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import java.nio.ByteBuffer;
import db.postgresql.protocol.v3.typeinfo.PgType;
import java.util.BitSet;

public class BitSetSerializer extends Serializer {

    public static final PgType PGTYPE_BIT =
        new PgType.Builder().name("bit").oid(1560).arrayId(1561).build();

    public static final PgType PGTYPE_VARBIT =
        new PgType.Builder().name("varbit").oid(1562).arrayId(1563).build();

    public static final BitSetSerializer instance = new BitSetSerializer();
    
    private BitSetSerializer() {
        super(BitSet.class);
    }
        
    public BitSet read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }

        BitSet ret = new BitSet(size);
        for(int i = 0; i < size; ++i) {
            final byte b = stream.get();
            final boolean val = (b == (byte) '1') ? true : false;
            ret.set(i, val);
        }

        return ret;
    }

    public int length(final BitSet bits, final Format format) {
        return bits.length();
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        return read(stream, size, format);
    }

    public void write(final Stream stream, final BitSet bits, final Format format) {
        for(int i = 0; i < bits.length(); ++i) {
            stream.put(bits.get(i) ? (byte) '1' : (byte) '0');
        }
    }

    public Bindable bindable(final BitSet bits, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return BitSetSerializer.this.length(bits, format); }
            public void write(final Stream stream) { BitSetSerializer.this.write(stream, bits, format); }
        };
    }
}
