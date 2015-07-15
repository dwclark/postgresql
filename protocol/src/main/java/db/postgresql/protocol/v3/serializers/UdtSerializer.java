package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;
import db.postgresql.protocol.v3.io.Stream;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.nio.charset.Charset;


public class UdtSerializer extends Serializer {

    private final Class<? extends Udt> type;
    private final Charset encoding;
    
    public UdtSerializer(final Class<? extends Udt> type, final Charset encoding) {
        super(oids(-1), classes(type));
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

    public int length(final TemporalAccessor date, final Format format) {
        throw new UnsupportedOperationException();
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        throw new UnsupportedOperationException();
    }

    public void write(final Stream stream, final LocalTime val, final Format format) {
        throw new UnsupportedOperationException();
    }
    
    public Bindable bindable(final LocalTime val, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return UdtSerializer.this.length(val, format); }
            public void write(final Stream stream) { UdtSerializer.this.write(stream, val, format); }
        };
    }
}
