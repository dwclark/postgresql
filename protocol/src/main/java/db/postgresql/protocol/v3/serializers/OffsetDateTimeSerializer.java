package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class OffsetDateTimeSerializer extends Serializer<OffsetDateTime> {

    private static final String STR = "uuuu-MM-dd HH:mm:ss.nx";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final PgType PGTYPE =
        new PgType.Builder().name("timestamptz").oid(1184).arrayId(1185).build();

    public static final OffsetDateTimeSerializer instance = new OffsetDateTimeSerializer();
    
    private OffsetDateTimeSerializer() {
        super(OffsetDateTime.class);
    }

    public OffsetDateTime read(final Stream stream, final int size, final Format format) {
        if(size == NULL_LENGTH) {
            return null;
        }
        else {
            final String str = _str(stream, size, ASCII_ENCODING);
            final int index = str.lastIndexOf('-');
            return OffsetDateTime.parse(str.substring(0, index) + "000" + str.substring(index), DATE);
        }
    }

    public int length(final OffsetDateTime date, final Format format) {
        return 32;
    }

    public void write(final Stream stream, final OffsetDateTime date, final Format format) {
        stream.putString(date.format(DATE));
    }
}
