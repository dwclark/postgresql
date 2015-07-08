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

public class LocalDateTimeSerializer extends Serializer {

    private static final String STR = "uuuu-MM-dd HH:mm:ss.n";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public LocalDateTimeSerializer() {
        super(oids(1114), classes(LocalDateTime.class));
    }

    public LocalDateTime read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }
        else {
            final String str = _str(stream, size, ASCII_ENCODING);
            return LocalDateTime.parse(str + "000", DATE);
        }
    }

    public int length(final TemporalAccessor date, final Format format) {
        return 29;
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        return read(stream, size, format);
    }

    public void write(final Stream stream, final LocalDateTime date, final Format format) {
        stream.putString(date.format(DATE));
    }
    
    public Bindable bindable(final LocalDateTime date, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return LocalDateTimeSerializer.this.length(date, format); }
            public void write(final Stream stream) { LocalDateTimeSerializer.this.write(stream, date, format); }
        };
    }
}
