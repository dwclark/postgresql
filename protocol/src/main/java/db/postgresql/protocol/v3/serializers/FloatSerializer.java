package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;
import db.postgresql.protocol.v3.io.Stream;
import java.nio.ByteBuffer;

public class FloatSerializer extends Serializer {

    public static final FloatSerializer instance = new FloatSerializer();
    
    private FloatSerializer() {
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
}
