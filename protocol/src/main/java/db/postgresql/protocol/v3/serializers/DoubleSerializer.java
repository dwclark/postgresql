package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.typeinfo.PgType;
import java.lang.reflect.Array;

public class DoubleSerializer extends Serializer<Double> {

    public static final PgType PGTYPE =
        new PgType.Builder().name("float8").oid(701).arrayId(1022).build();

    public static final DoubleSerializer instance = new DoubleSerializer();
    
    private DoubleSerializer() {
        super(Double.class);
    }

    @Override
    public Class getArrayType() {
        return double.class;
    }

    @Override
    public void putArray(final Object ary, final int index, final String val) {
        Array.setDouble(ary, index, Double.parseDouble(val));
    }

    public Double fromString(final String str) {
        return Double.valueOf(str);
    }
    
    public double readPrimitive(final Stream stream, final int size) {
        return size == NULL_LENGTH ? 0.0d : Double.valueOf(str(stream, size, ASCII_ENCODING));
    }

    public Double read(final Stream stream, final int size) {
        return size == NULL_LENGTH ? null : readPrimitive(stream, size);
    }

    public int lengthPrimitive(final double d) {
        return Double.toString(d).length();
    }

    public int length(final Double d) {
        return lengthPrimitive(d);
    }

    public void writePrimitive(final Stream stream, final double val) {
        stream.putString(Double.toString(val));
    }

    public void write(final Stream stream, final Double val) {
        writePrimitive(stream, val);
    }

    public Bindable bindable(final double val) {
        return new Bindable() {
            public int getLength() { return lengthPrimitive(val); }
            public void write(final Stream stream) { writePrimitive(stream, val); }
        };
    }
}
