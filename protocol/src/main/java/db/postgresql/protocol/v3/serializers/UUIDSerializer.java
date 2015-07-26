package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.typeinfo.PgType;
import java.util.UUID;

public class UUIDSerializer extends Serializer<UUID> {

    public static final PgType PGTYPE =
        new PgType.Builder().name("uuid").oid(2950).arrayId(2951).build();

    public static final UUIDSerializer instance = new UUIDSerializer();
    
    private UUIDSerializer() {
        super(UUID.class);
    }

    public UUID read(final Stream stream, final int size, final Format format) {
        return isNull(size) ? null : UUID.fromString(_str(stream, size, ASCII_ENCODING));
    }

    public int length(final UUID val, final Format format) {
        return val.toString().length();
    }

    public void write(final Stream stream, final UUID val, final Format format) {
        stream.putString(val.toString());
    }
}
