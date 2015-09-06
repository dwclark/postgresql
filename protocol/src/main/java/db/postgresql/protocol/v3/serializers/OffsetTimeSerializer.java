package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class OffsetTimeSerializer extends Serializer<OffsetTime> {

    private static final String STR = "HH:mm:ss.nx";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final PgType PGTYPE =
         new PgType.Builder().name("timetz").oid(1266).arrayId(1270).build();

    public static final OffsetTimeSerializer instance = new OffsetTimeSerializer();
    
    private OffsetTimeSerializer() {
        super(OffsetTime.class);
    }

    public OffsetTime fromString(final String str) {
        final int index = str.lastIndexOf('-');
        return OffsetTime.parse(str.substring(0, index) + "000" + str.substring(index), DATE);
    }

    public OffsetTime read(final Stream stream, final int size) {
        return isNull(size) ? null : fromString(str(stream, size, ASCII_ENCODING));
    }

    public int length(final OffsetTime date) {
        return 18;
    }

    public void write(final Stream stream, final OffsetTime val) {
        stream.putString(val.format(DATE));
    }
}
