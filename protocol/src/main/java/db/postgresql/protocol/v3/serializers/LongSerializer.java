package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.Format;

public class LongSerializer extends Serializer {

    public static final LongSerializer instance = new LongSerializer();
    
    private LongSerializer() {
        super(oids(20, 21,23), classes(short.class, Short.class, int.class, Integer.class,
                                       long.class, Long.class));
    }

    private static final long[] powers = { 1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L,
                                           100000000L, 1000000000L, 10000000000L, 100000000000L,
                                           1000000000000L, 10000000000000L, 100000000000000L,
                                           1000000000000000L, 10000000000000000L,
                                           100000000000000000L, 1000000000000000000L, Long.MAX_VALUE };

    private static long pow(int i) {
        assert(i < 19);
        return powers[i];
    }

    public long read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return 0L;
        }
            
        long accum = 0;
        int place = size;
        boolean negate = false;
        
        for(int i = (size - 1); i >= 0; --i) {
            byte val = stream.get(i);
            if(val == '-') {
                negate = true;
            }
            else {
                accum += Character.digit(val, 10) * pow(i);
            }
        }

        return accum;
    }

    public int length(final long val, final Format format) {
        if(val == Long.MIN_VALUE) {
            return 20;
        }

        final long absValue = Math.abs(val);
        final boolean includeSign = (val != absValue);
        int digits;
        for(digits = 1; digits < (powers.length - 1); ++digits) {
            if(absValue < powers[digits]) {
                break;
            }
        }

        return digits + (includeSign ? 1 : 0);
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }
        else {
            return read(stream, size, format);
        }
    }
}
