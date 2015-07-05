package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.Format;

public class IntSerializer extends Serializer {

    public static final IntSerializer instance = new IntSerializer();
    
    private IntSerializer() {
        super(oids(21,23), classes(short.class, Short.class, int.class, Integer.class));
    }

    private static final int[] powers = { 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000,
                                          10_000_000, 100_000_000, 1_000_000_000, Integer.MAX_VALUE };

    private static int pow(int i) {
        assert(i < 11);
        return powers[i];
    }

    public int read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return 0;
        }
            
        int accum = 0;
        int place = size;
        boolean negate = false;
        
        for(int i = (size - 1); i >= 0; --i) {
            byte val = stream.get();
            if(val == '-') {
                negate = true;
            }
            else {
                accum += Character.digit(val, 10) * pow(i);
            }
        }

        return accum;
    }

    public int length(final int val, final Format format) {
        if(val == Integer.MIN_VALUE) {
            return 11;
        }

        final int absValue = Math.abs(val);
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
