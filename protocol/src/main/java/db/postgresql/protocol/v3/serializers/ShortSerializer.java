package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.Format;

public class ShortSerializer extends Serializer {

    public static final ShortSerializer instance = new ShortSerializer();
    
    private ShortSerializer() {
        super(oids(21), classes(short.class, Short.class));
    }

    private final static short[] powers = { 1, 10, 100, 1_000, 10_000, Short.MAX_VALUE };

    private static short pow(int i) {
        assert(i < 6);
        return powers[i];
    }
    
    public short read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return 0;
        }
            
        short accum = 0;
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

    public int length(final short val, final Format f) {
        if(val == Short.MIN_VALUE) {
            return 6;
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
