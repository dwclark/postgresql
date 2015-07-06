package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;
import db.postgresql.protocol.v3.io.Stream;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateTimeSerializer extends Serializer {

    public static final DateTimeSerializer instance = new DateTimeSerializer();

    private static final String STR = "uuuu-MM-dd HH:mm:ss.n";
    private static final DateTimeFormatter LOCAL = DateTimeFormatter.ofPattern(STR);
    private static final DateTimeFormatter OFFSET = DateTimeFormatter.ofPattern(STR + "x");

    private DateTimeSerializer() {
        super(oids(1114), classes(LocalDateTime.class,OffsetDateTime.class));
    }

    public TemporalAccessor read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }

        String str = _str(stream, size, ASCII_ENCODING);
        final int index = str.lastIndexOf('-');
        if(str.lastIndexOf('-') > 7) {
            return OFFSET.parse(str.substring(0, index) + "000" + str.substring(index));
        }
        else {
            return LOCAL.parse(str);
        }
    }

    public int length(final TemporalAccessor date, final Format format) {
        return (date instanceof LocalDateTime) ? 29 : 32;
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        return read(stream, size, format);
    }

    public void write(final Stream stream, final TemporalAccessor date, final Format format) {
        if(date instanceof LocalDateTime) {
            stream.putString(LOCAL.format(date));
        }
        else if(date instanceof LocalDateTime) {
            stream.putString(OFFSET.format(date));
        }
        else {
            throw new IllegalArgumentException("date must be either LocalDateTime or OffsetDateTime");
        }
    }
    
    public Bindable bindable(final TemporalAccessor date, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return instance.length(date, format); }
            public void write(final Stream stream) { instance.write(stream, date, format); }
        };
    }
}
