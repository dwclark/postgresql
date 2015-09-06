package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class LocalTimeSerializer extends Serializer<LocalTime> {

    private static final String STR = "HH:mm:ss.n";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final PgType PGTYPE =
        new PgType.Builder().name("time").oid(1083).arrayId(1183).build();

    public static final LocalTimeSerializer instance = new LocalTimeSerializer();
    
    private LocalTimeSerializer() {
        super(LocalTime.class);
    }

    public LocalTime fromString(final String str) {
        return LocalTime.parse(str + "000", DATE);
    }

    public LocalTime read(final Stream stream, final int size) {
        return isNull(size) ? null : fromString(str(stream, size, ASCII_ENCODING));
    }

    public int length(final LocalTime time) {
        return 15;
    }

    public void write(final Stream stream, final LocalTime val) {
        stream.putString(val.format(DATE));
    }
}
