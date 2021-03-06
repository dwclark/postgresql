package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.typeinfo.PgType;
import java.lang.reflect.Array;

public class IntSerializer extends Serializer<Integer> {

    public static final PgType PGTYPE =
        new PgType.Builder().name("int4").oid(23).arrayId(1007).build();

    public static final IntSerializer instance = new IntSerializer();

    @Override
    public Class getArrayType() {
        return int.class;
    }

    @Override
    public void putArray(final Object ary, final int index, final String val) {
        Array.setInt(ary, index, Integer.parseInt(val));
    }

    private IntSerializer() {
        super(Integer.class);
    }

    private static final int[] powers = { 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000,
                                          10_000_000, 100_000_000, 1_000_000_000, Integer.MAX_VALUE };

    private static int pow(int i) {
        assert(i < 11);
        return powers[i];
    }

    public Integer fromString(final String str) {
        return Integer.valueOf(str);
    }
    
    public int readPrimitive(final Stream stream, final int size) {
        if(size == NULL_LENGTH) {
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

        return negate ? -accum : accum;
    }

    public Integer read(final Stream stream, final int size) {
        return size == NULL_LENGTH ? null : readPrimitive(stream, size);
    }

    public int lengthPrimitive(final int val) {
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

    public int length(final Integer val) {
        return val == null ? NULL_LENGTH : lengthPrimitive(val);
    }

    public static final byte[] DIGITS = { (byte) '0', (byte) '1', (byte) '2', (byte) '3', (byte) '4',
                                          (byte) '5', (byte) '6', (byte) '7', (byte) '8', (byte) '9' };
    
    public void writePrimitive(final Stream stream, final int val) {
        final int size = length(val);
        final byte[] bytes = new byte[size];
        final int startAt = size - 1;
        final int endAt = (val < 0) ? 1 : 0;
        
        int accum = val;
        for(int i = startAt; i >= endAt; --i) {
            bytes[i] = DIGITS[Math.abs(accum % 10)];
            accum /= 10;
        }

        if(endAt == 1) {
            bytes[0] = (byte) '-';
        }

        stream.put(bytes);
    }

    public void write(final Stream stream, final Integer val) {
        writePrimitive(stream, val);
    }

    public Bindable bindable(final int val) {
        return new Bindable() {
            public int getLength() { return lengthPrimitive(val); }
            public void write(final Stream stream) { writePrimitive(stream, val); }
        };
    }
}
