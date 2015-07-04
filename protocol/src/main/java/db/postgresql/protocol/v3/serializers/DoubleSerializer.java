package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import java.nio.ByteBuffer;

public class DoubleSerializer extends Serializer {
    public DoubleSerializer() {
        super(oids(700,701), classes(float.class, Float.class, double.class, Double.class));
    }

    public double read(final ByteBuffer buffer, final Extent extent, final Format format) {
        if(extent.isNull()) {
            return 0.0f;
        }

        String str = _str(buffer, extent, ASCII_ENCODING);
        switch(str) {
        case NaN: return Double.NaN;
        case POSITIVE_INFINITY: return Double.POSITIVE_INFINITY;
        case NEGATIVE_INFINITY: return Double.NEGATIVE_INFINITY;
        default: return Double.valueOf(str);
        }
    }

    public int length(final double d, final Format format) {
        return Double.toString(d).length();
    }
}
