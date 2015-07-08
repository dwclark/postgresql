package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public class TimeSerializer extends Serializer {

    public static final TimeSerializer instance = new TimeSerializer();

    private static final String STR = "HH:mm:ss.n";
    private static final DateTimeFormatter LOCAL = DateTimeFormatter.ofPattern(STR);
    private static final DateTimeFormatter OFFSET = DateTimeFormatter.ofPattern(STR + "x");

    public TimeSerializer() {
        super(oids(1083,1266), classes(LocalTime.class, OffsetTime.class));
    }

    public TemporalAccessor read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }
        
        final String str = _str(stream, size, ASCII_ENCODING);
        final int index = str.lastIndexOf('-');
        if(index == -1) {
            return LocalTime.parse(str + "000", LOCAL);
        }
        else {
            return OffsetTime.parse(str.substring(0, index) + "000" + str.substring(index), OFFSET);
        }
    }

    public int length(final TemporalAccessor date, final Format format) {
        if(date instanceof LocalTime) {
            return 15;
        }
        else {
            return 18;
        }
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        return read(stream, size, format);
    }

    public void write(final Stream stream, final TemporalAccessor val, final Format format) {
        if(val instanceof LocalTime) {
            LocalTime lt = (LocalTime) val;
            stream.putString(lt.format(LOCAL));
        }
        else if(val instanceof OffsetTime) {
            OffsetTime ot = (OffsetTime) val;
            stream.putString(ot.format(OFFSET));
        }
        else {
            throw new IllegalArgumentException("val is not LocalTime or OffsetTime");
        }
    }
    
    public Bindable bindable(final TemporalAccessor val, final boolean hasTimeZone, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return TimeSerializer.this.length(val, format); }
            public void write(final Stream stream) { TimeSerializer.this.write(stream, val, format); }
        };
    }
}
