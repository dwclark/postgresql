package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.typeinfo.PgType;
import java.lang.reflect.Array;

public class BooleanSerializer extends Serializer<Boolean> {

    public static final byte T = (byte) 't';
    public static final byte F = (byte) 'f';

    public static final PgType PGTYPE =
        new PgType.Builder().name("bool").oid(16).arrayId(1000).build();

    public static final BooleanSerializer instance = new BooleanSerializer();

    @Override
    public Class getArrayType() {
        return boolean.class;
    }

    @Override
    public void putArray(final Object ary, final int index, final String val) {
        Array.setBoolean(ary, index, val.charAt(0) == T ? true : false);
    }
    
    private BooleanSerializer() {
        super(Boolean.class);
    }

    public Boolean fromString(final String str) {
        return str.charAt(0) == T;
    }
    
    public boolean readPrimitive(final Stream stream, final int size) {
        if(size == NULL_LENGTH) {
            return false;
        }
        
        return (stream.get() == T) ? true : false;
    }

    public Boolean read(final Stream stream, final int size) {
        return isNull(size) ? null : readPrimitive(stream, size);
    }

    public int lengthPrimitive(final boolean b) {
        return 1;
    }

    public int length(final Boolean b) {
        return b == null ? NULL_LENGTH : lengthPrimitive(b);
    }

    public void writePrimitive(final Stream stream, final boolean val) {
        if(val) {
            stream.put(T);
        }
        else {
            stream.put(F);
        }
    }
    
    public void write(final Stream stream, final Boolean val) {
        writePrimitive(stream, val);
    }

    public Bindable bindable(final boolean val) {
        return new Bindable() {
            public int getLength() { return lengthPrimitive(val); }
            public void write(final Stream stream) { writePrimitive(stream, val); }
        };
    }
}
