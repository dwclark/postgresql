package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class BooleanSerializer extends Serializer {

    public static final byte T = (byte) 't';
    public static final byte F = (byte) 'f';

    public static final PgType PGTYPE =
        new PgType.Builder().name("bool").oid(16).arrayId(1000).build();

    public static final BooleanSerializer instance = new BooleanSerializer();
    
    private BooleanSerializer() {
        super(boolean.class, Boolean.class);
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
            public int getLength() { return BooleanSerializer.this.length(val, format); }
            public void write(final Stream stream) { BooleanSerializer.this.write(stream, val, format); }
        };
    }
}
