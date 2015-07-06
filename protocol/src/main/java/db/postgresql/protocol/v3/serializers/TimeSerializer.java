package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.sql.Time;
import db.postgresql.protocol.v3.ProtocolException;
import java.text.SimpleDateFormat;

public class TimeSerializer extends Serializer {

    public static final TimeSerializer instance = new TimeSerializer();
    
    public static final String ISO_STR = "(\\d\\d):(\\d\\d):(\\d\\d)\\.(\\d{6})(([\\-|\\+]\\d\\d)?)";
    public static final Pattern ISO = Pattern.compile(ISO_STR);
    public static final String NO_TZ_OUTPUT = "hh:mm:ss";
    public static final String TZ_OUTPUT = NO_TZ_OUTPUT + "X";
    
    private TimeSerializer() {
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
        
    public Time read(final Stream stream, final int size, final Format format) {
        return isNull(size) ? null : iso(_str(stream, size, ASCII_ENCODING));
    }

    public int length(final Time t, final boolean hasTimeZone, final Format format) {
        return hasTimeZone ? 11 : 8;
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        return read(stream, size, format);
    }

    public void write(final Stream stream, final Time val, boolean hasTimeZone, final Format format) {
        final SimpleDateFormat sdf = hasTimeZone ? new SimpleDateFormat(TZ_OUTPUT) : new SimpleDateFormat(NO_TZ_OUTPUT);
        stream.putString(sdf.format(val));
    }
    
    public Bindable bindable(final Time val, final boolean hasTimeZone, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return instance.length(val, hasTimeZone, format); }
            public void write(final Stream stream) { instance.write(stream, val, hasTimeZone, format); }
        };
    }
}
