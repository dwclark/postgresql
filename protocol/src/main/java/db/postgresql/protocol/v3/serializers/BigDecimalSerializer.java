package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import java.math.BigDecimal;

public class BigDecimalSerializer extends Serializer {

    public static final BigDecimalSerializer instance = new BigDecimalSerializer();
    
    private BigDecimalSerializer() {
        super(oids(790,1700), classes(BigDecimal.class));
    }

    public BigDecimal read(final Stream stream, final int size, final Format format) {
        return isNull(size) ? null : new BigDecimal(_str(stream, size, ASCII_ENCODING));
    }

    public int length(final BigDecimal bd, final Format format) {
        return (bd == null) ? -1 : bd.toString().length();
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        return read(stream, size, format);
    }
}
