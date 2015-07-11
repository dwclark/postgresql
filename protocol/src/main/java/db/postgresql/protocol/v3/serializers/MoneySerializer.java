package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Currency;
import java.util.Locale;

public class MoneySerializer extends Serializer {

    private final Locale locale;
    
    public MoneySerializer(final Locale locale) {
        super(oids(790), classes(Money.class));
        this.locale = locale;
    }

    public DecimalFormat getFormatter() {
        DecimalFormat formatter = (DecimalFormat) NumberFormat.getCurrencyInstance(locale);
        //formatter.setCurrency(Currency.getInstance(locale));
        formatter.setParseBigDecimal(true);
        return formatter;
    }

    public Money read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }
        else {
            final String str = _str(stream, size, ASCII_ENCODING);
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

    public Object readObject(final Stream stream, final int size, final Format format) {
        return read(stream, size, format);
    }

    public void write(final Stream stream, final Money money, final Format format) {
        stream.putString(toString(money.unwrap()));
    }

    public Bindable bindable(final Money money, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return MoneySerializer.this.length(money, format); }
            public void write(final Stream stream) { MoneySerializer.this.write(stream, money, format); }
        };
    }
}
