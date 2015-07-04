package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;
import java.nio.ByteBuffer;

public class FloatSerializer extends Serializer {
    public FloatSerializer() {
        super(oids(700), classes(float.class, Float.class));
    }

    public float read(final ByteBuffer buffer, final Extent extent, final Format format) {
        if(extent.isNull()) {
            return 0.0f;
        }

        String str = _str(buffer, extent, ASCII_ENCODING);
        switch(str) {
        case NaN: return Float.NaN;
        case POSITIVE_INFINITY: return Float.POSITIVE_INFINITY;
        case NEGATIVE_INFINITY: return Float.NEGATIVE_INFINITY;
        default: return Float.valueOf(str);
        }
    }

    public int length(final float val, final Format format) {
        return Float.toString(val).length();
    }
}
