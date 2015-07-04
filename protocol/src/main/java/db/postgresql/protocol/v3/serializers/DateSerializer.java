package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.Date;
import db.postgresql.protocol.v3.ProtocolException;

public class DateSerializer extends Serializer {
    public static final String ISO_STR = "(\\d{4})-(\\d{2})-(\\d{2})";
    public static final Pattern ISO = Pattern.compile(ISO_STR);
    
    public DateSerializer() {
        super(oids(1082), classes(Date.class));
    }

    public static Date iso(final String str) {
        final Matcher m = ISO.matcher(str);
        if(!m.matches()) {
            throw new ProtocolException(str + " is not a valid iso date format");
        }

        final Calendar cal = Calendar.getInstance();
        final int years = Integer.valueOf(m.group(1));
        final int months = Integer.valueOf(m.group(2));
        final int days = Integer.valueOf(m.group(3));
        cal.set(years, months, days, 0, 0, 0);
        return new Date(cal.getTime().getTime());
    }

    public Date read(final ByteBuffer buffer, final Extent extent, final Format format) {
        if(extent.isNull()) {
            return null;
        }

        return iso(_str(buffer, extent, ASCII_ENCODING));
    }

    public int length(final Date d, final Format format) {
        //yyyy-mm-dd -> 10
        return 10;
    }
}
