package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.io.Stream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class StringSerializer extends Serializer<String> {

    private final Charset encoding;
    private final CharsetEncoder encoder;

    public static final PgType PGTYPE_TEXT =
        new PgType.Builder().name("text").oid(25).arrayId(1009).build();
    public static final PgType PGTYPE_VARCHAR =
        new PgType.Builder().name("varchar").oid(1043).arrayId(1015).build();

    public StringSerializer(final Charset encoding) {
        super(String.class);
        this.encoding = encoding;
        this.encoder = encoding.newEncoder();
    }

    public String fromString(final String str) {
        return str;
    }
    
    public String read(final Stream stream, final int size) {
        return size == NULL_LENGTH ? null : str(stream, size);
    }

    public int length(final String str) {
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

    public void write(final Stream stream, final String val) {
        stream.putString(val);
    }
}
