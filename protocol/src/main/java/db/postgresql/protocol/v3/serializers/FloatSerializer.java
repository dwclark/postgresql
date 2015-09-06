package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.typeinfo.PgType;
import java.lang.reflect.Array;

public class FloatSerializer extends Serializer<Float> {
    
    public static final PgType PGTYPE =
        new PgType.Builder().name("float4").oid(700).arrayId(1021).build();

    public static final FloatSerializer instance = new FloatSerializer();
    
    private FloatSerializer() {
        super(Float.class);
    }

    @Override
    public Class getArrayType() {
        return float.class;
    }

    @Override
    public void putArray(final Object ary, final int index, final String val) {
        Array.setFloat(ary, index, Float.parseFloat(val));
    }
    
    public Float fromString(final String str) {
        return Float.valueOf(str);
    }
    
    public float readPrimitive(final Stream stream, final int size) {
        return size == NULL_LENGTH ? 0.0f : Float.valueOf(str(stream, size, ASCII_ENCODING));
    }

    public Float read(final Stream stream, final int size) {
        return size == NULL_LENGTH ? null : readPrimitive(stream, size);
    }
    
    public int lengthPrimitive(final float val) {
        return Float.toString(val).length();
    }

    public int length(final Float val) {
        return val == null ? NULL_LENGTH : lengthPrimitive(val);
    }

    public void writePrimitive(final Stream stream, final float val) {
        stream.putString(Float.toString(val));
    }

    public void write(final Stream stream, final Float val) {
        writePrimitive(stream, val);
    }

    public Bindable bindable(final float val) {
        return new Bindable() {
            public int getLength() { return lengthPrimitive(val); }
            public void write(final Stream stream) { writePrimitive(stream, val); }
        };
    }
}
