package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class FloatSerializer extends Serializer<Float> {
    
    public static final PgType PGTYPE =
        new PgType.Builder().name("float4").oid(700).arrayId(1021).build();

    public static final FloatSerializer instance = new FloatSerializer();
    
    private FloatSerializer() {
        super(float.class, Float.class);
    }

    public float readPrimitive(final Stream stream, final int size, final Format format) {
        return size == NULL_LENGTH ? 0.0f : Float.valueOf(str(stream, size, ASCII_ENCODING));
    }

    public Float read(final Stream stream, final int size, final Format format) {
        return size == NULL_LENGTH ? null : readPrimitive(stream, size, format);
    }
    
    public int lengthPrimitive(final float val, final Format format) {
        return Float.toString(val).length();
    }

    public int length(final Float val, final Format format) {
        return val == null ? NULL_LENGTH : lengthPrimitive(val, format);
    }

    public void writePrimitive(final Stream stream, final float val, final Format format) {
        stream.putString(Float.toString(val));
    }

    public void write(final Stream stream, final Float val, final Format format) {
        writePrimitive(stream, val, format);
    }

    public Bindable bindable(final float val, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return lengthPrimitive(val, format); }
            public void write(final Stream stream) { writePrimitive(stream, val, format); }
        };
    }
}
