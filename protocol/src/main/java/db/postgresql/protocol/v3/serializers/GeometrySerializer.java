package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import java.nio.charset.Charset;

public class GeometrySerializer<T extends Udt> extends Serializer<T> {

    public GeometrySerializer(final Class<T> type) {
        super(type);
    }

    public T fromString(final String s) {
        UdtParser parser = UdtParser.forGeometry(s);
        return parser.readUdt(getType());
    }
    
    public T read(final Stream stream, final int size) {
        return isNull(size) ? null : fromString(str(stream, size));
    }

    public int length(final T val) {
        throw new UnsupportedOperationException();
    }

    public void write(final Stream stream, final T val) {
        throw new UnsupportedOperationException();
    }
}
