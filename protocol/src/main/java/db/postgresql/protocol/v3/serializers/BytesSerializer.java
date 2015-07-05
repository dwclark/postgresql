package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;

public class BytesSerializer extends Serializer {

    public static final BytesSerializer instance = new BytesSerializer();
    
    private BytesSerializer() {
        super(oids(17), classes(byte.class, byte[].class));
    }
        
    public byte[] read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }

        stream.get(); stream.get(); //advance past escape sequence
        byte[] ret = new byte[(size - 2) / 2];
        for(int i = 0; i < (size - 2); ++i) {
            ret[i] = (byte) ((Character.digit(stream.get(), 16) * 16) + Character.digit(stream.get(), 16));
        }

        return ret;
    }

    public int length(final byte[] bytes, final Format format) {
        return (bytes == null) ? -1 : (2 + (bytes.length * 2));
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        return read(stream, size, format);
    }
}
