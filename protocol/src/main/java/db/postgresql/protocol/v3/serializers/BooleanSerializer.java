package db.postgresql.protocol.v3.serializers;

import java.nio.ByteBuffer;
import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;

public class BooleanSerializer extends Serializer {

    private static final byte T = (byte) 't';
    private static final byte F = (byte) 'f';
    
    public BooleanSerializer() {
        super(oids(16), classes(boolean.class, Boolean.class));
    }
    
    public boolean read(final ByteBuffer buffer, final Extent extent, final Format format) {
        return buffer.get(extent.getPosition()) == T ? true : false;
    }
    
    public int length(final boolean b, final Format format) {
        return 1;
    }
}
