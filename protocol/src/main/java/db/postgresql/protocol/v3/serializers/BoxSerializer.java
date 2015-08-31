package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import java.nio.charset.Charset;
import db.postgresql.protocol.v3.types.Box;
import db.postgresql.protocol.v3.types.Point;

public class BoxSerializer extends Serializer<Box> {

    private final Charset encoding;
    
    public BoxSerializer(final Charset encoding) {
        super(Box.class);
        this.encoding = encoding;
    }

    public static Box from(final String buffer) {
        final int mid = buffer.indexOf(',', buffer.indexOf(',') + 1);
        final String first = buffer.substring(0, mid);
        final String second = buffer.substring(mid + 1);
        return new Box(UdtParser.forGeometry(first).readUdt(Point.class),
                       UdtParser.forGeometry(second).readUdt(Point.class));
    }

    public Box read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }
        else {
            return from(_str(stream, size));
        }
    }

    public int length(final Box val, final Format format) {
        throw new UnsupportedOperationException();
    }

    public void write(final Stream stream, final Box val, final Format format) {
        throw new UnsupportedOperationException();
    }
}
