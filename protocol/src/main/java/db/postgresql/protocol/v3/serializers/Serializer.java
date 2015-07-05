package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Collections;
import java.util.HashMap;

public abstract class Serializer {

    private final int[] oids;

    public int[] getOids() {
        return oids;
    }

    private final Class[] types;
    
    public Class[] getTypes() {
        return types;
    }

    public boolean handles(final int val) {
        for(int oid : oids) {
            if(val == oid) {
                return true;
            }
        }

        return false;
    }

    public void checkHandles(final int val) {
        if(!handles(val)) {
            throw new ProtocolException(getClass().getName() + " can't handle oid " + val);
        }
    }

    public Serializer(final int[] oids, final Class... types) {
        this.oids = oids;
        this.types = types;
    }

    public abstract Object readObject(Stream stream, int size, Format format);

    protected static int[] oids(int... vals) {
        return vals;
    }

    protected static Class[] classes(Class... vals) {
        return vals;
    }

    protected static final Charset ASCII_ENCODING = Charset.forName("US-ASCII");

    private static class StringArea extends ThreadLocal<byte[]> {
        @Override
        protected byte[] initialValue() {
            return new byte[1024];
        }

        public byte[] ensure(int size) {
            if(get().length < size) {
                set(new byte[size]);
            }

            return get();
        }
    }

    private static final StringArea _stringArea = new StringArea();
    
    protected static String _str(final Stream stream, final int size, final Charset encoding) {
        byte[] bytes = stream.get(_stringArea.ensure(size), 0, size);
        return new String(bytes, 0, size, encoding);
    }

    protected static String _str(final Stream stream, final int size) {
        return _str(stream, size, stream.getEncoding());
    }

    protected static boolean isNull(final int size) {
        return size == -1;
    }
}
