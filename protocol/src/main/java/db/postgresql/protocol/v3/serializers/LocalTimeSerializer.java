package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;
import db.postgresql.protocol.v3.io.Stream;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class LocalTimeSerializer extends Serializer {

    private static final String STR = "HH:mm:ss.n";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final PgType PGTYPE =
        new PgType.Builder().name("time").oid(1083).arrayId(1183).build();

    public static final LocalTimeSerializer instance = new LocalTimeSerializer();
    
    private LocalTimeSerializer() {
        super(LocalTime.class);
    }

    public LocalTime read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }
        else {
            return LocalTime.parse(_str(stream, size, ASCII_ENCODING) + "000", DATE);
        }
    }

    public int length(final TemporalAccessor date, final Format format) {
        return 15;
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        return read(stream, size, format);
    }

    public void write(final Stream stream, final LocalTime val, final Format format) {
        stream.putString(val.format(DATE));
    }
    
    public Bindable bindable(final LocalTime val, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return LocalTimeSerializer.this.length(val, format); }
            public void write(final Stream stream) { LocalTimeSerializer.this.write(stream, val, format); }
        };
    }
}
