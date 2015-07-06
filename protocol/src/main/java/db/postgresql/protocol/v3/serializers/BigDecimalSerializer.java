package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
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

    public void write(final Stream stream, final BigDecimal bd, final Format format) {
        stream.putString(bd.toString());
    }

    public Bindable bindable(final BigDecimal bd, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return instance.length(bd, format); }
            public void write(final Stream stream) { instance.write(stream, bd, format); }
        };
    }
}
