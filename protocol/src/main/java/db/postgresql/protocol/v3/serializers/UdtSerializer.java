package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import java.nio.charset.Charset;

public class UdtSerializer<T extends Udt> extends Serializer<T> {

    private final Class<T> type;
    private final Charset encoding;
    
    public UdtSerializer(final Class<T> type, final Charset encoding) {
        super(type);
        this.type = type;
        this.encoding = encoding;
    }

    public T fromString(final String str) {
        UdtParser parser = UdtParser.forUdt(str);
        return parser.readUdt(type);
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
