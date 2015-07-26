package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class DoubleSerializer extends Serializer<Double> {

    public static final PgType PGTYPE =
        new PgType.Builder().name("float8").oid(701).arrayId(1022).build();

    public static final DoubleSerializer instance = new DoubleSerializer();
    
    private DoubleSerializer() {
        super(double.class, Double.class);
    }

    public double readPrimitive(final Stream stream, final int size, final Format format) {
        return size == NULL_LENGTH ? 0.0d : Double.valueOf(_str(stream, size, ASCII_ENCODING));
    }

    public Double read(final Stream stream, final int size, final Format format) {
        return size == NULL_LENGTH ? null : readPrimitive(stream, size, format);
    }

    public int lengthPrimitive(final double d, final Format format) {
        return Double.toString(d).length();
    }

    public int length(final Double d, final Format format) {
        return lengthPrimitive(d, format);
    }

    public void writePrimitive(final Stream stream, final double val, final Format format) {
        stream.putString(Double.toString(val));
    }

    public void write(final Stream stream, final Double val, final Format format) {
        writePrimitive(stream, val, format);
    }

    public Bindable bindable(final double val, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return lengthPrimitive(val, format); }
            public void write(final Stream stream) { writePrimitive(stream, val, format); }
        };
    }
}
