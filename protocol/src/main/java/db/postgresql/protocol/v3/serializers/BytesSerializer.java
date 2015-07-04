package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import java.nio.ByteBuffer;

public class BytesSerializer extends Serializer {

    public BytesSerializer() {
        super(oids(17), classes(byte.class, byte[].class));
    }
        
    public byte[] read(final ByteBuffer buffer, final Extent extent, final Format format) {
        if(extent.isNull()) {
            return null;
        }

        final int total = (extent.size - 2) / 2;
        final int base = extent.position + 2;
        byte[] ret = new byte[total];
        for(int i = 0; i < total; ++i) {
            final int shift = i * 2;
            int first = buffer.get(base + shift);
            int second = buffer.get(base + shift + 1);
            ret[i] = (byte) ((Character.digit(first, 16) * 16) + Character.digit(second, 16));
        }

        return ret;
    }

    public int length(final byte[] bytes, final Format format) {
        return 2 + (bytes.length * 2);
    }
}
