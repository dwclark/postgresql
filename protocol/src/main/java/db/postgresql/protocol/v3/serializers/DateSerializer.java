package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;
import db.postgresql.protocol.v3.io.Stream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DateSerializer extends Serializer {

    public static final DateSerializer instance = new DateSerializer();
    private static final String STR = "uuuu-MM-dd";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);
    
    private DateSerializer() {
        super(oids(1082), classes(LocalDate.class));
    }
    
    public LocalDate read(final Stream stream, final int size, final Format format) {
        if(isNull(size)) {
            return null;
        }
        else {
            return LocalDate.parse(_str(stream, size, ASCII_ENCODING), DATE);
        }
    }

    public int length(final LocalDate d, final Format format) {
        //yyyy-mm-dd -> 10
        return (d == null) ? -1 : 10;
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        return read(stream, size, format);
    }

    public void write(final Stream stream, final LocalDate val, final Format format) {
        stream.putString(val.format(DATE));
    }

    public Bindable bindable(final LocalDate val, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return instance.length(val, format); }
            public void write(final Stream stream) { instance.write(stream, val, format); }
        };
    }
}
