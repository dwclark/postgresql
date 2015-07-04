package db.postgresql.protocol.v3.serializers;

import java.nio.ByteBuffer;
import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;

public class IntSerializer extends Serializer {

    
    public IntSerializer() {
        super(oids(21,23), classes(short.class, Short.class, int.class, Integer.class));
    }

    public int read(final ByteBuffer buffer, final Extent extent, final Format format) {
        if(extent.isNull()) {
            return 0;
        }
            
        int accum = 0;
        int multiplier = 1;
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

    private static final int[] sizes = { 9, 99, 999, 9999, 99999, 999999, 9999999,
                                         99999999, 999999999, Integer.MAX_VALUE };
    public int length(final int val, final Format format) {
        if(val == Integer.MIN_VALUE) {
            return 11;
        }

        final int absValue = Math.abs(val);
        final boolean includeSign = (val != absValue);
        for(int i = 0; i < sizes.length; ++i) {
            if(absValue <= sizes[i]) {
                return (i + 1 + (includeSign ? 1 : 0));
            }
        }

        throw new RuntimeException(); //should never happen, keep the compiler happy
    }
}
