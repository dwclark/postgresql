package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.CharBuffer;

public class StringSerializer extends Serializer {

    public StringSerializer() {
        super(oids(25,1043), classes(String.class));
    }
        
    public String read(final ByteBuffer buffer, final Extent extent,
                       final Format format, final Charset encoding) {
        if(extent.isNull()) {
            return null;
        }

        return _str(buffer, extent, encoding);
    }

    public int length(final String str, final Format format, final Charset encoding) {
        final CharBuffer charBuffer = CharBuffer.wrap(str);
        final ByteBuffer tmp = ByteBuffer.allocate(64);
        final CharsetEncoder encoder = encoding.newEncoder();
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
}
