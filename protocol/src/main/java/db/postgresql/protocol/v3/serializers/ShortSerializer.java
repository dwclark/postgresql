package db.postgresql.protocol.v3.serializers;

import java.nio.ByteBuffer;
import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;

public class ShortSerializer extends Serializer {

    public ShortSerializer() {
        super(oids(21), classes(short.class, Short.class));
    }

    public short read(final ByteBuffer buffer, final Extent extent, final Format format) {
        if(extent.isNull()) {
            return 0;
        }
            
        short accum = 0;
        short multiplier = 1;
        for(int i = extent.getLast(); i >= extent.getPosition(); --i) {
            byte val = buffer.get(i);
            if(val == '-') {
                return (short) -accum;
            }
            else {
                accum += (Character.digit(val, 10) * multiplier);
                multiplier *= 10;
            }
        }

        return accum;
    }

    private final static short[] sizes = { 9, 99, 999, 9999, Short.MAX_VALUE };

    public int length(final short s, final Format f) {
        if(s == Short.MIN_VALUE) {
            return 6;
        }

        final short absValue = (short) Math.abs(s);
        final boolean includeSign = absValue != s;
        for(int i = 0; i < sizes.length; ++i) {
            if(absValue <= sizes[i]) {
                return (i + 1 + (includeSign ? 1 : 0));
            }
        }

        throw new RuntimeException(); //should never get here, but make the compiler happy
    }
}
