package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.Bindable;

public class DoubleSerializer extends Serializer {

    public static final DoubleSerializer instance = new DoubleSerializer();
    
    private DoubleSerializer() {
        super(oids(700,701), classes(float.class, Float.class, double.class, Double.class));
    }

    public double read(final Stream stream, final int size, final Format format) {
        return isNull(size) ? 0.0d : Double.valueOf(_str(stream, size, ASCII_ENCODING));
    }

    public int length(final double d, final Format format) {
        return Double.toString(d).length();
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }
        else {
            return read(stream, size, format);
        }
    }

    public void write(final Stream stream, final double val, final Format format) {
        stream.putString(Double.toString(val));
    }

    public Bindable bindable(final double val, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return instance.length(val, format); }
            public void write(final Stream stream) { instance.write(stream, val, format); }
        };
    }
}
