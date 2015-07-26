package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Format;
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

    public LocalTime read(final Stream stream, final int size, final Format format) {
        return size == NULL_LENGTH ? null : LocalTime.parse(_str(stream, size, ASCII_ENCODING) + "000", DATE);
    }

    public int length(final LocalTime time, final Format format) {
        return 15;
    }

    public void write(final Stream stream, final LocalTime val, final Format format) {
        stream.putString(val.format(DATE));
    }
}
