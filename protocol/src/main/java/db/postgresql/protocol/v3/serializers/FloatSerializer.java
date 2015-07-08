package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;
import db.postgresql.protocol.v3.io.Stream;

public class FloatSerializer extends Serializer {

    public FloatSerializer() {
        super(oids(700), classes(float.class, Float.class));
    }

    public float read(final Stream stream, final int size, final Format format) {
        return isNull(size) ? 0.0f : Float.valueOf(_str(stream, size, ASCII_ENCODING));
    }

    public int length(final float val, final Format format) {
        return Float.toString(val).length();
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }
        else {
            return read(stream, size, format);
        }
    }

    public void write(final Stream stream, final float val, final Format format) {
        stream.putString(Float.toString(val));
    }

    public Bindable bindable(final float val, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return FloatSerializer.this.length(val, format); }
            public void write(final Stream stream) { FloatSerializer.this.write(stream, val, format); }
        };
    }
}
