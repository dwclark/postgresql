package db.postgresql.protocol.v3.serializers;

import java.nio.ByteBuffer;
import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;

public class LongSerializer extends Serializer {
    public LongSerializer() {
        super(oids(20, 21,23), classes(short.class, Short.class, int.class, Integer.class,
                                       long.class, Long.class));
    }

    public long read(final ByteBuffer buffer, final Extent extent, final Format format) {
        if(extent.isNull()) {
            return 0L;
        }
            
        long accum = 0;
        long multiplier = 1;
        for(int i = extent.getLast(); i >= extent.getPosition(); --i) {
            byte val = buffer.get(i);
            if(val == '-') {
                return -accum;
            }
            else {
                accum += (Character.digit(val, 10) * multiplier);
                multiplier *= 10;
            }
        }

        return accum;
    }

    private static final long[] sizes = { 9L, 99L, 999L, 9999L, 99999L, 999999L, 9999999L,
                                          99999999L, 999999999L, 9999999999L, 99999999999L,
                                          999999999999L, 9999999999999L, 99999999999999L, 999999999999999L,
                                          9999999999999999L, 99999999999999999L, 999999999999999999L,
                                          Long.MAX_VALUE };

    public int length(final long l, final Format format) {
        if(l == Long.MIN_VALUE) {
            return 20;
        }

        final long absValue = Math.abs(l);
        final boolean includeSign = (l != absValue);
        for(int i = 0; i < sizes.length; ++i) {
            if(absValue <= sizes[i]) {
                return (i + 1 + (includeSign ? 1 : 0));
            }
        }

        throw new RuntimeException(); //keep the compiler happy
    }

}
