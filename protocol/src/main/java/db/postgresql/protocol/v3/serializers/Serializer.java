package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import java.nio.charset.Charset;
import java.lang.reflect.Array;

public abstract class Serializer<T> {

    private final Class<T> type;
    
    public Class<T> getType() {
        return type;
    }

    public Class getArrayType() {
        return type;
    }

    public void putArray(final Object ary, final int index, final String val) {
        Array.set(ary, index, fromString(val));
    }

    public Serializer(final Class<T> type) {
        this.type = type;
    }

    public abstract T fromString(String str);
    public abstract T read(Stream stream, int size);
    public abstract void write(Stream stream, T val);
    public abstract int length(T val);

    public Object readArray(final Stream stream, final int size, final char delimiter) {
        return new ArrayParser(str(stream, size), this, delimiter).toArray();
    }

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

    public static Bindable bindable() {
        return new Bindable() {
            public int getLength() { return -1; }
            public void write(final Stream stream) { }
        };
    }

    public Bindable bindable(final T val) {
        return new Bindable() {

            public int getLength() {
                if(val == null) {
                    return NULL_LENGTH;
                }
                else {
                    return length(val);
                }
            }
            
            public void write(final Stream stream) { Serializer.this.write(stream, val); }
        };
    }
}
