package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Locale;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class MoneySerializer extends Serializer<Money> {

    public static final PgType PGTYPE =
        new PgType.Builder().name("money").oid(790).arrayId(791).build();

    private final Locale locale;

    public MoneySerializer(final Locale locale) {
        this.locale = locale;
    }

    public DecimalFormat getFormatter() {
        DecimalFormat formatter = (DecimalFormat) NumberFormat.getCurrencyInstance(locale);
        formatter.setParseBigDecimal(true);
        return formatter;
    }

    public Money read(final Stream stream, final int size, final Format format) {
        if(size == NULL_LENGTH) {
            return null;
        }
        else {
            final String str = str(stream, size, ASCII_ENCODING);
            return Money.wrap((BigDecimal) getFormatter().parse(str, new ParsePosition(0)));
        }
    }

    public String toString(final BigDecimal bd) {
        StringBuffer sb = new StringBuffer();
        getFormatter().format(bd, sb, new FieldPosition(0));
        return sb.toString();
    }
    
    public int length(final Money money, final Format format) {
        return (money == null) ? -1 : toString(money.unwrap()).length();
    }

    public void write(final Stream stream, final Money money, final Format format) {
        stream.putString(toString(money.unwrap()));
    }
}
