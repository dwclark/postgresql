package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.typeinfo.PgType;
import java.util.BitSet;

public class BitSetSerializer extends Serializer<BitSet> {

    public static final PgType PGTYPE_BIT =
        new PgType.Builder().name("bit").oid(1560).arrayId(1561).build();

    public static final PgType PGTYPE_VARBIT =
        new PgType.Builder().name("varbit").oid(1562).arrayId(1563).build();

    public static final BitSetSerializer instance = new BitSetSerializer();
    
    private BitSetSerializer() {
        super(BitSet.class);
    }
        
    public int length(final BitSet bits) {
        return (bits == null) ? NULL_LENGTH : bits.length();
    }

    public BitSet fromString(final String str) {
        final int size = str.length();
        BitSet ret = new BitSet(size);
        for(int i = 0; i < size; ++i) {
            ret.set(i, str.charAt(i) == '1' ? true : false);
        }
        
        return ret;
    }
    
    public BitSet read(final Stream stream, final int size) {
        if(size == NULL_LENGTH) {
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

    public void write(final Stream stream, final BitSet bits) {
        for(int i = 0; i < bits.length(); ++i) {
            stream.put(bits.get(i) ? (byte) '1' : (byte) '0');
        }
    }
}
