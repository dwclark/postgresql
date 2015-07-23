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

public class OffsetTimeSerializer extends Serializer {

    private static final String STR = "HH:mm:ss.nx";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final PgType PGTYPE =
         new PgType.Builder().name("timetz").oid(1266).arrayId(1270).build();

    public static final OffsetTimeSerializer instance = new OffsetTimeSerializer();
    
    private OffsetTimeSerializer() {
        super(OffsetTime.class);
    }

    public OffsetTime read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }
        else {
            final String str = _str(stream, size, ASCII_ENCODING);
            final int index = str.lastIndexOf('-');
            return OffsetTime.parse(str.substring(0, index) + "000" + str.substring(index), DATE);
        }
    }

    public int length(final OffsetTime date, final Format format) {
        return 18;
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        return read(stream, size, format);
    }

    public void write(final Stream stream, final OffsetTime val, final Format format) {
        stream.putString(val.format(DATE));
    }
    
    public Bindable bindable(final OffsetTime val, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return OffsetTimeSerializer.this.length(val, format); }
            public void write(final Stream stream) { OffsetTimeSerializer.this.write(stream, val, format); }
        };
    }
}
