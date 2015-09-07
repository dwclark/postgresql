package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Session;
import db.postgresql.protocol.v3.io.Stream;
import java.nio.charset.Charset;

public class GeometrySerializer<T extends Udt> extends Serializer<T> {

    private final Session session;
    
    public GeometrySerializer(final Session session, final Class<T> type) {
        super(type);
        this.session = session;
    }

    public T fromString(final String s) {
        UdtParser parser = UdtParser.forGeometry(session, s);
        return parser.read(getType());
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
