package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;

public class BooleanSerializer extends Serializer {

    public static final BooleanSerializer instance = new BooleanSerializer();
    
    private static final byte T = (byte) 't';
    private static final byte F = (byte) 'f';
    
    private BooleanSerializer() {
        super(oids(16), classes(boolean.class, Boolean.class));
    }
    
    public boolean read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return false;
        }
        
        return (stream.get() == T) ? true : false;
    }
    
    public int length(final boolean b, final Format format) {
        return 1;
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }
        else {
            return read(stream, size, format);
        }
    }

    public void write(final Stream stream, final boolean val, final Format format) {
        if(val) {
            stream.put(T);
        }
        else {
            stream.put(F);
        }
    }

    public Bindable bindable(final boolean val, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return instance.length(val, format); }
            public void write(final Stream stream) { instance.write(stream, val, format); }
        };
    }
}
