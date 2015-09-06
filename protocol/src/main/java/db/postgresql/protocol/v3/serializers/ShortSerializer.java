package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.typeinfo.PgType;
import java.lang.reflect.Array;

public class ShortSerializer extends Serializer<Short> {

    public static final PgType PGTYPE =
        new PgType.Builder().name("int2").oid(21).arrayId(1005).build();

    public static final ShortSerializer instance = new ShortSerializer();

    @Override
    public Class getArrayType() {
        return short.class;
    }
    
    @Override
    public void putArray(final Object ary, final int index, final String val) {
        Array.setShort(ary, index, Short.parseShort(val));
    }
    
    private ShortSerializer() {
        super(Short.class);
    }
    
    private final static short[] powers = { 1, 10, 100, 1_000, 10_000, Short.MAX_VALUE };

    private static short pow(int i) {
        assert(i < 6);
        return powers[i];
    }

    public Short fromString(final String str) {
        return Short.valueOf(str);
    }
    
    public short readPrimitive(final Stream stream, final int size) {
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

        return negate ? (short) -accum : accum;
    }

    public Short read(final Stream stream, final int size) {
        return size == NULL_LENGTH ? null : readPrimitive(stream, size);
    }

    public int lengthPrimitive(final short val) {
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

    public int length(final Short val) {
        return val == null ? NULL_LENGTH : lengthPrimitive(val);
    }

    public void writePrimitive(final Stream stream, final short val) {
        final int size = length(val);
        final byte[] bytes = new byte[size];
        final int startAt = size - 1;
        final int endAt = (val < 0) ? 1 : 0;
        
        short accum = val;
        for(int i = startAt; i >= endAt; --i) {
            bytes[i] = IntSerializer.DIGITS[Math.abs((int) (accum % 10))];
            accum /= 10;
        }

        if(endAt == 1) {
            bytes[0] = (byte) '-';
        }

        stream.put(bytes);
    }

    public void write(final Stream stream, final Short val) {
        writePrimitive(stream, val);
    }

    public Bindable bindable(final short val) {
        return new Bindable() {
            public int getLength() { return lengthPrimitive(val); }
            public void write(final Stream stream) { writePrimitive(stream, val); }
        };
    }
}
