package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.typeinfo.PgType;
import java.util.UUID;

public class UUIDSerializer extends Serializer {

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

    public Object readObject(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }
        else {
            return read(stream, size, format);
        }
    }

    public void write(final Stream stream, final UUID val, final Format format) {
        stream.putString(val.toString());
    }

    public Bindable bindable(final UUID val, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return UUIDSerializer.this.length(val, format); }
            public void write(final Stream stream) { UUIDSerializer.this.write(stream, val, format); }
        };
    }
}
