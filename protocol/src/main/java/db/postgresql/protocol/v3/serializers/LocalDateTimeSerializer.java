package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class LocalDateTimeSerializer extends Serializer<LocalDateTime> {

    private static final String STR = "uuuu-MM-dd HH:mm:ss.n";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final LocalDateTimeSerializer instance = new LocalDateTimeSerializer();
    
    public static final PgType PGTYPE =
        new PgType.Builder().name("timestamp").oid(1114).arrayId(1115).build();

    private LocalDateTimeSerializer() {
        super(LocalDateTime.class);
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

    public int length(final LocalDateTime date, final Format format) {
        return 29;
    }

    public void write(final Stream stream, final LocalDateTime date, final Format format) {
        stream.putString(date.format(DATE));
    }
}
