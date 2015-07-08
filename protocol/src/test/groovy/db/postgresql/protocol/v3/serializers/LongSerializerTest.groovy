package db.postgresql.protocol.v3.serializers;

import spock.lang.*;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.FixedStream;
import java.nio.charset.Charset;
import java.nio.ByteBuffer;

class LongSerializerTest extends Specification {
    
    def s = LongSerializer.instance;
    def f = Format.TEXT;
    def ascii = Charset.forName('US-ASCII');

    @Ignore
    def "Test Length"() {
        expect:
        s.length(0, f) == 1;
        s.length(Long.MIN_VALUE, f) == 20;
        s.length(Long.MAX_VALUE, f) == 19;
        s.length(9_999_999_999, f) == 10;
        s.length(10_000_000_000, f) == 11;
        s.length(-9_999_999_999, f) == 11;
        s.length(-10_000_000_000, f) == 12;
        s.length(9L, f) == 1;
        s.length(10L, f) == 2;
    }

    private String toString(final FixedStream fs) {
        ByteBuffer buffer = fs.sendBuffer;
        buffer.flip();
        String str = new String(buffer.array(), 0, buffer.limit(), ascii);
        buffer.clear();
        return str;
    }

    private void strToStream(final FixedStream fs, final String str) {
        fs.recvBuffer.clear();
        fs.recvBuffer.put(str.getBytes(ascii));
        fs.recvBuffer.flip();
    }

    @Ignore
    def "Test Write"() {
        setup:
        def fs = new FixedStream(40, ascii);

        when:
        s.write(fs, 100, f);
        then:
        toString(fs) == '100';

        when:
        s.write(fs, Long.MAX_VALUE, f);
        then:
        toString(fs)  == '9223372036854775807';

        when:
        s.write(fs, 99_999, f);
        then:
        toString(fs) == '99999';

        when:
        s.write(fs, -1_000_000_000_001L, f);
        then:
        toString(fs) == '-1000000000001';
    }

    def "Test Read"() {
        setup:
        def fs = new FixedStream(40, ascii);

        when:
        strToStream(fs, "999999");
        then:
        s.read(fs, 6, f) == 999_999;

        when:
        strToStream(fs, '-9223372036854775808');
        then:
        s.read(fs, 20, f) == -9223372036854775808L;

        when:
        strToStream(fs, '9223372036854775807');
        then:
        s.read(fs, 19, f) == 9223372036854775807L;

        when:
        strToStream(fs, '-12345');
        then:
        s.read(fs, 6, f) == -12345;
    }
}
