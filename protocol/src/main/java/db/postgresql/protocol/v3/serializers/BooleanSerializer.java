package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class BooleanSerializer extends Serializer<Boolean> {

    public static final byte T = (byte) 't';
    public static final byte F = (byte) 'f';

    public static final PgType PGTYPE =
        new PgType.Builder().name("bool").oid(16).arrayId(1000).build();

    public static final BooleanSerializer instance = new BooleanSerializer();
    
    private BooleanSerializer() {
        super(boolean.class, Boolean.class);
    }
    
    public boolean readPrimitive(final Stream stream, final int size, final Format format) {
        if(size == NULL_LENGTH) {
            return false;
        }
        
        return (stream.get() == T) ? true : false;
    }

    public Boolean read(final Stream stream, final int size, final Format format) {
        return isNull(size) ? null : readPrimitive(stream, size, format);
    }

    public int lengthPrimitive(final boolean b, final Format format) {
        return 1;
    }

    public int length(final Boolean b, final Format format) {
        return b == null ? NULL_LENGTH : lengthPrimitive(b, format);
    }

    public void writePrimitive(final Stream stream, final boolean val, final Format format) {
        if(val) {
            stream.put(T);
        }
        else {
            stream.put(F);
        }
    }
    
    public void write(final Stream stream, final Boolean val, final Format format) {
        writePrimitive(stream, val, format);
    }

    public Bindable bindable(final boolean val, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return lengthPrimitive(val, format); }
            public void write(final Stream stream) { writePrimitive(stream, val, format); }
        };
    }
}
