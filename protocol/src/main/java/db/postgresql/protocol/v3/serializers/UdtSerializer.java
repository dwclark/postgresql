package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import java.nio.charset.Charset;


public class UdtSerializer extends Serializer<Udt> {

    private final Class<? extends Udt> type;
    private final Charset encoding;
    
    public UdtSerializer(final Class<? extends Udt> type, final Charset encoding) {
        super(type);
        this.type = type;
        this.encoding = encoding;
    }

    public Udt read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }
        else {
            UdtParser parser = new UdtParser(_str(stream, size));
            return parser.readUdt(type);
        }
    }

    public int length(final Udt val, final Format format) {
        throw new UnsupportedOperationException();
    }

    public void write(final Stream stream, final Udt val, final Format format) {
        throw new UnsupportedOperationException();
    }
}
