package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.CharBuffer;
import java.nio.ByteBuffer;

public class StringSerializer extends Serializer {

    private final Charset encoding;
    private final CharsetEncoder encoder;
    
    public StringSerializer(final Charset encoding) {
        super(oids(25,1043), classes(String.class));
        this.encoding = encoding;
        this.encoder = encoding.newEncoder();
    }
        
    public String read(final Stream stream, final int size, final Format format) {
        return isNull(size) ? null : _str(stream, size);
    }

    public int length(final String str, final Format format) {
        encoder.reset();
        final CharBuffer charBuffer = CharBuffer.wrap(str);
        final ByteBuffer tmp = ByteBuffer.allocate(64);
        int total = 0;
        while(charBuffer.hasRemaining()) {
            encoder.encode(charBuffer, tmp, false);
            total += tmp.position();
            tmp.clear();
        }

        while(encoder.encode(charBuffer, tmp, true) != CoderResult.UNDERFLOW) {
            total += tmp.position();
            tmp.clear();
        }

        total += tmp.position();
        return total;
    }

    public Object readObject(final Stream stream, final int size, final Format format) {
        return read(stream, size, format);
    }

    public void write(final Stream stream, final String val, final Format format) {
        stream.putString(val);
    }
    
    public Bindable bindable(final String val, final Format format) {
        return new Bindable() {
            public Format getFormat() { return format; }
            public int getLength() { return StringSerializer.this.length(val, format); }
            public void write(final Stream stream) { StringSerializer.this.write(stream, val, format); }
        };
    }
}
