package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Format;
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

    public T read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }
        else {
            UdtParser parser = UdtParser.forUdt(str(stream, size));
            return parser.readUdt(type);
        }
    }

    public int length(final T val, final Format format) {
        throw new UnsupportedOperationException();
    }

    public void write(final Stream stream, final T val, final Format format) {
        throw new UnsupportedOperationException();
    }
}
