package db.postgresql.protocol.v3.serializers;

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

    public UUID fromString(final String str) {
        return UUID.fromString(str);
    }

    public UUID read(final Stream stream, final int size) {
        return isNull(size) ? null : fromString(str(stream, size, ASCII_ENCODING));
    }

    public int length(final UUID val) {
        return val.toString().length();
    }

    public void write(final Stream stream, final UUID val) {
        stream.putString(val.toString());
    }
}
