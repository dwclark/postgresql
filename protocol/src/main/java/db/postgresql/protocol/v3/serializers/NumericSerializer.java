package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Locale;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class NumericSerializer extends Serializer {

    public static final PgType PGTYPE =
        new PgType.Builder().name("numeric").oid(1700).arrayId(1231).build();

    private final Locale locale;

    public NumericSerializer(final Locale locale) {
        super(BigDecimal.class);
        this.locale = locale;
    }

    public DecimalFormat getFormatter() {
        DecimalFormat formatter = (DecimalFormat) NumberFormat.getInstance(locale);
        formatter.setParseBigDecimal(true);
        return formatter;
    }

    public BigDecimal read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }
        else {
            return (BigDecimal) getFormatter().parse(_str(stream, size, ASCII_ENCODING), new ParsePosition(0));
        }
    }

    public String toString(final BigDecimal bd) {
        StringBuffer sb = new StringBuffer();
        getFormatter().format(bd, sb, new FieldPosition(0));
        return sb.toString();
    }

    public int length(final BigDecimal bd, final Format format) {
        return (bd == null) ? -1 : toString(bd).length();
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        return read(stream, size, format);
    }

    public void write(final Stream stream, final BigDecimal bd, final Format format) {
        stream.putString(toString(bd));
    }

    public Bindable bindable(final BigDecimal bd, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return NumericSerializer.this.length(bd, format); }
            public void write(final Stream stream) { NumericSerializer.this.write(stream, bd, format); }
        };
    }
}
