package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class DateSerializer extends Serializer<LocalDate> {

    private static final String STR = "uuuu-MM-dd";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final PgType PGTYPE =
        new PgType.Builder().name("date").oid(1082).arrayId(1182).build();

    public static final DateSerializer instance = new DateSerializer();
    
    private DateSerializer() {
        super(LocalDate.class);
    }

    public LocalDate fromString(final String str) {
        return LocalDate.parse(str, DATE);
    }
    
    public LocalDate read(final Stream stream, final int size) {
        return isNull(size) ? null : fromString(str(stream, size, ASCII_ENCODING));
    }

    public int length(final LocalDate d) {
        //yyyy-mm-dd -> 10
        return (d == null) ? NULL_LENGTH : 10;
    }

    public void write(final Stream stream, final LocalDate val) {
        stream.putString(val.format(DATE));
    }
}
