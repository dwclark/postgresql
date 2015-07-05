package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.Format;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.sql.Timestamp;
import db.postgresql.protocol.v3.ProtocolException;
    
public class TimestampSerializer extends Serializer {

    public static final TimestampSerializer instance = new TimestampSerializer();
    
    public static final String ISO_STR = DateSerializer.ISO_STR + " " + TimeSerializer.ISO_STR;
    public static final Pattern ISO = Pattern.compile(ISO_STR);

    private static Calendar cal(final MatchResult result) {
        if(result.groupCount() == 7) {
            return Calendar.getInstance();
        }
        else {
            return Calendar.getInstance(TimeZone.getTimeZone("GMT" + result.group(8)));
        }
    }
        
    private TimestampSerializer() {
        super(oids(1114), classes(Timestamp.class));
    }

    public static Timestamp iso(final String str) {
        final Matcher m = ISO.matcher(str);
        if(!m.matches()) {
            throw new ProtocolException(str + " is not a recognized time format");
        }

        final MatchResult result = m.toMatchResult();
        final int years = Integer.valueOf(m.group(1));
        final int months = Integer.valueOf(m.group(2));
        final int days = Integer.valueOf(m.group(3));
        final int hours = Integer.valueOf(m.group(4));
        final int minutes = Integer.valueOf(m.group(5));
        final int seconds = Integer.valueOf(m.group(6));
        final int nanos = Integer.valueOf(m.group(7)) * 1000;

        final Calendar cal = cal(result);
        cal.set(years, months, days, hours, minutes, seconds);
        final Timestamp tstamp = new Timestamp(cal.getTime().getTime());
        tstamp.setNanos(nanos);
        return tstamp;
    }
        
    public Timestamp read(final Stream stream, final int size, final Format format) {
        return isNull(size) ? null : iso(_str(stream, size, ASCII_ENCODING));
    }

    public int length(final Timestamp t, final Format format) {
        return 26;
    }

    public int length(final Timestamp t, final TimeZone tz, final Format format) {
        return length(t, format) + 3;
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        return read(stream, size, format);
    }
}
