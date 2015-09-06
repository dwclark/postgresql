package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Locale;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class NumericSerializer extends Serializer<BigDecimal> {

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

    public BigDecimal fromString(final String str) {
        return new BigDecimal(str);
    }
    
    public BigDecimal read(final Stream stream, final int size) {
        if(isNull(size)) {
            return null;
        }
        else {
            return (BigDecimal) getFormatter().parse(str(stream, size, ASCII_ENCODING), new ParsePosition(0));
        }
    }

    public String toString(final BigDecimal bd) {
        StringBuffer sb = new StringBuffer();
        getFormatter().format(bd, sb, new FieldPosition(0));
        return sb.toString();
    }

    public int length(final BigDecimal bd) {
        return (bd == null) ? -1 : toString(bd).length();
    }

    public void write(final Stream stream, final BigDecimal bd) {
        stream.putString(toString(bd));
    }
}
