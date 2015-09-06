package db.postgresql.protocol.v3.serializers;

import spock.lang.*;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.FixedStream;
import java.nio.charset.Charset;
import java.nio.ByteBuffer;

class LongSerializerTest extends Specification {
    
    def s = new LongSerializer();
    def f = Format.TEXT;
    def ascii = Charset.forName('US-ASCII');

    def "Test Length"() {
        expect:
        s.length(0) == 1;
        s.length(Long.MIN_VALUE) == 20;
        s.length(Long.MAX_VALUE) == 19;
        s.length(9_999_999_999) == 10;
        s.length(10_000_000_000) == 11;
        s.length(-9_999_999_999) == 11;
        s.length(-10_000_000_000) == 12;
        s.length(9L) == 1;
        s.length(10L) == 2;
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

    def "Test Write"() {
        setup:
        def fs = new FixedStream(40, ascii);

        when:
        s.write(fs, 100);
        then:
        toString(fs) == '100';

        when:
        s.write(fs, Long.MAX_VALUE);
        then:
        toString(fs)  == '9223372036854775807';

        when:
        s.write(fs, 99_999);
        then:
        toString(fs) == '99999';

        when:
        s.write(fs, -1_000_000_000_001L);
        then:
        toString(fs) == '-1000000000001';
    }

    def "Test Read"() {
        setup:
        def fs = new FixedStream(40, ascii);

        when:
        strToStream(fs, "999999");
        then:
        s.read(fs, 6) == 999_999;

        when:
        strToStream(fs, '-9223372036854775808');
        then:
        s.read(fs, 20) == -9223372036854775808L;

        when:
        strToStream(fs, '9223372036854775807');
        then:
        s.read(fs, 19) == 9223372036854775807L;

        when:
        strToStream(fs, '-12345');
        then:
        s.read(fs, 6) == -12345;
    }
}
