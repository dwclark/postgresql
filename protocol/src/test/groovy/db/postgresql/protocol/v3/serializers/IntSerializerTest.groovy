package db.postgresql.protocol.v3.serializers;

import spock.lang.*;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.FixedStream;
import java.nio.charset.Charset;
import java.nio.ByteBuffer;

class IntSerializerTest extends Specification {
    
    def s = new IntSerializer();
    def f = Format.TEXT;
    def ascii = Charset.forName('US-ASCII');

    def "Test Length"() {
        expect:
        s.length(0) == 1;
        s.length(Integer.MIN_VALUE) == 11;
        s.length(Integer.MAX_VALUE) == 10;
        s.length(9_999_999) == 7;
        s.length(10_000_000) == 8;
        s.length(-9_999_999) == 8;
        s.length(-10_000_000) == 9;
        s.length(9) == 1;
        s.length(10) == 2;
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
        def fs = new FixedStream(20, ascii);

        when:
        s.write(fs, 100);
        then:
        toString(fs) == '100';

        when:
        s.write(fs, Integer.MAX_VALUE);
        then:
        toString(fs)  == '2147483647';

        when:
        s.write(fs, 99_999);
        then:
        toString(fs) == '99999';

        when:
        s.write(fs, -1_000_000_001);
        then:
        toString(fs) == '-1000000001';
    }

    def "Test Read"() {
        setup:
        def fs = new FixedStream(20, ascii);

        when:
        strToStream(fs, "999999");
        then:
        s.read(fs, 6) == 999_999;

        when:
        strToStream(fs, '-2147483648');
        then:
        s.read(fs, 11) == -2147483648;

        when:
        strToStream(fs, '2147483647');
        then:
        s.read(fs, 10) == 2147483647;

        when:
        strToStream(fs, '-12345');
        then:
        s.read(fs, 6) == -12345;
    }
}
