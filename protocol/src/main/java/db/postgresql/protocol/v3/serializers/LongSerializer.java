package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;

public class LongSerializer extends Serializer {

    public LongSerializer() {
        super(oids(20), classes(long.class, Long.class));
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

        return negate ? -accum : accum;
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

    public void write(final Stream stream, final long val, final Format format) {
        final int size = length(val, format);
        final byte[] bytes = new byte[size];
        final int startAt = size - 1;
        final int endAt = (val < 0) ? 1 : 0;
        
        long accum = val;
        for(int i = startAt; i >= endAt; --i) {
            bytes[i] = IntSerializer.DIGITS[Math.abs((int) (accum % 10))];
            accum /= 10;
        }

        if(endAt == 1) {
            bytes[0] = (byte) '-';
        }

        stream.put(bytes);
    }

    public Bindable bindable(final long val, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return LongSerializer.this.length(val, format); }
            public void write(final Stream stream) { LongSerializer.this.write(stream, val, format); }
        };
    }
}
