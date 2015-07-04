package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;
import java.nio.charset.Charset;
import java.nio.ByteBuffer;
public abstract class Serializer {

    private final int[] oids;

    public int[] getOids() {
        return oids;
    }

    private final Class[] types;
    
    public Class[] getTypes() {
        return types;
    }

    public Serializer(final int[] oids, final Class... types) {
        this.oids = oids;
        this.types = types;
    }

    protected static int[] oids(int... vals) {
        return vals;
    }

    protected static Class[] classes(Class... vals) {
        return vals;
    }

    protected static final String POSITIVE_INFINITY = "Infinity";
    protected static final String NEGATIVE_INFINITY = "-Infinity";
    protected static final String NaN = "Nan";
    protected static final Charset ASCII_ENCODING = Charset.forName("US-ASCII");
    
    protected static String _str(final ByteBuffer buffer, final Extent extent, final Charset encoding) {
        final int startAt = buffer.arrayOffset() + extent.position;
        return new String(buffer.array(), startAt, extent.size, encoding);
    }
}
