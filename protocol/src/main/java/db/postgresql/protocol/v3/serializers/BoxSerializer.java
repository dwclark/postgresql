package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Session;
import db.postgresql.protocol.v3.io.Stream;
import java.nio.charset.Charset;
import db.postgresql.protocol.v3.types.Box;
import db.postgresql.protocol.v3.types.Point;

public class BoxSerializer extends Serializer<Box> {

    private final Session session;
    
    public BoxSerializer(final Session session) {
        super(Box.class);
        this.session = session;
    }

    public Box fromString(final String buffer) {
        final int mid = buffer.indexOf(',', buffer.indexOf(',') + 1);
        final String first = buffer.substring(0, mid);
        final String second = buffer.substring(mid + 1);
        return new Box(UdtParser.forGeometry(session, first).read(Point.class),
                       UdtParser.forGeometry(session, second).read(Point.class));
    }

    public Box read(final Stream stream, final int size) {
        return isNull(size) ? null : fromString(str(stream, size));
    }

    public int length(final Box val) {
        throw new UnsupportedOperationException();
    }

    public void write(final Stream stream, final Box val) {
        throw new UnsupportedOperationException();
    }
}
