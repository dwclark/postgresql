package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import java.nio.ByteBuffer;
import java.math.BigDecimal;

public class BigDecimalSerializer extends Serializer {
    public BigDecimalSerializer() {
        super(oids(790,1700), classes(BigDecimal.class));
    }

    public BigDecimal read(final ByteBuffer buffer, final Extent extent, final Format format) {
        if(extent.isNull()) {
            return null;
        }

        String str = _str(buffer, extent, ASCII_ENCODING);
        return new BigDecimal(str);
    }

    public int length(final BigDecimal bd, final Format format) {
        return bd.toString().length();
    }
}
