package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.typeinfo.PgType;
import java.lang.reflect.Array;

public class LongSerializer extends Serializer<Long> {

    public static final PgType PGTYPE =
        new PgType.Builder().name("int8").oid(20).arrayId(1016).build();

    public static final LongSerializer instance = new LongSerializer();
    
    private LongSerializer() {
        super(Long.class);
    }

    @Override
    public Class getArrayType() {
        return long.class;
    }

    @Override
    public void putArray(final Object ary, final int index, final String val) {
        Array.setLong(ary, index, Long.parseLong(val));
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

    public long readPrimitive(final Stream stream, final int size) {
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

    public Long fromString(final String str) {
        return Long.valueOf(str);
    }
    
    public Long read(final Stream stream, final int size) {
        return size == NULL_LENGTH ? null : readPrimitive(stream, size);
    }

    public int lengthPrimitive(final long val) {
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

    public int length(final Long val) {
        return val == null ? NULL_LENGTH : lengthPrimitive(val);
    }

    public void writePrimitive(final Stream stream, final long val) {
        final int size = length(val);
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

    public void write(final Stream stream, final Long val) {
        writePrimitive(stream, val);
    }

    public Bindable bindable(final long val) {
        return new Bindable() {
            public int getLength() { return lengthPrimitive(val); }
            public void write(final Stream stream) { writePrimitive(stream, val); }
        };
    }
}
