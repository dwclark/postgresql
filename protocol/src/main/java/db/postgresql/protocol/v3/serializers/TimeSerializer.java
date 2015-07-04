package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.sql.Time;
import db.postgresql.protocol.v3.ProtocolException;

public class TimeSerializer extends Serializer {

    public static final String ISO_STR = "(\\d\\d):(\\d\\d):(\\d\\d)\\.(\\d{6})(([\\-|\\+]\\d\\d)?)";
    public static final Pattern ISO = Pattern.compile(ISO_STR);
    
    public TimeSerializer() {
        super(oids(1083), classes(Time.class));
    }

    private static Calendar cal(final MatchResult result) {
        if(result.groupCount() == 4) {
            return Calendar.getInstance();
        }
        else {
            return Calendar.getInstance(TimeZone.getTimeZone("GMT" + result.group(5)));
        }
    }

    public static Time iso(final String str) {
        final Matcher m = ISO.matcher(str);
        if(!m.matches()) {
            throw new ProtocolException(str + " is not a recognized time format");
        }

        final MatchResult result = m.toMatchResult();
        final int hours = Integer.valueOf(m.group(1));
        final int minutes = Integer.valueOf(m.group(2));
        final int micros = Integer.valueOf(m.group(4));
        final int seconds = Integer.valueOf(m.group(3)) + ((micros >= 500_000) ? 1 : 0);

        final Calendar cal = cal(result);
        cal.set(0, 0, 0, hours, minutes, seconds);
        return new Time(cal.getTime().getTime());
    }
        
    public Time read(final ByteBuffer buffer, final Extent extent, final Format format) {
        if(extent.isNull()) {
            return null;
        }

        return iso(_str(buffer, extent, ASCII_ENCODING));
    }

    public int length(final Time t, final Format format) {
        //hh:mm:ss -> 8
        return 8;
    }

    public int length(final Time t, final TimeZone tz, final Format format) {
        return length(t, format) + 3;
    }
}
