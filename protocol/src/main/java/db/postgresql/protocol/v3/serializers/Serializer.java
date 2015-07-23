package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;
import db.postgresql.protocol.v3.io.Stream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class Serializer {

    private final Class[] types;
    
    public Class[] getTypes() {
        return types;
    }

    public Serializer(final Class... types) {
        this.types = types;
    }

    public abstract Object readObject(Stream stream, int size, Format format);

    public static final Charset ASCII_ENCODING = Charset.forName("US-ASCII");

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

    public Bindable nullBindable(final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return -1; }
            public void write(final Stream stream) { }
        };
    }
}
