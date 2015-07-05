package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.Extent;
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
}
