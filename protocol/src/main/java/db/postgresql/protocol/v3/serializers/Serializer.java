package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import java.nio.charset.Charset;

public abstract class Serializer<T> {

    private final Class[] types;
    
    public Class[] getTypes() {
        return types;
    }

    public Serializer(final Class... types) {
        this.types = types;
    }

    public abstract T read(Stream stream, int size, Format format);
    public abstract void write(Stream stream, T val, Format format);
    public abstract int length(T val, Format format);
    
    public static final Charset ASCII_ENCODING = Charset.forName("US-ASCII");
    public static final int NULL_LENGTH = -1;
    
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

    private static final StringArea stringArea = new StringArea();
    
    protected static String str(final Stream stream, final int size, final Charset encoding) {
        byte[] bytes = stream.get(stringArea.ensure(size), 0, size);
        return new String(bytes, 0, size, encoding);
    }

    protected static String str(final Stream stream, final int size) {
        return str(stream, size, stream.getEncoding());
    }

    protected static boolean isNull(final int size) {
        return size == NULL_LENGTH;
    }

    public static Bindable bindable(final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return -1; }
            public void write(final Stream stream) { }
        };
    }

    public Bindable bindable(final T val, final Format format) {
        return new Bindable() {

            public Format getFormat() { return format; }

            public int getLength() {
                if(val == null) {
                    return NULL_LENGTH;
                }
                else {
                    return length(val, format);
                }
            }
            
            public void write(final Stream stream) { Serializer.this.write(stream, val, format); }
        };
    }
}
