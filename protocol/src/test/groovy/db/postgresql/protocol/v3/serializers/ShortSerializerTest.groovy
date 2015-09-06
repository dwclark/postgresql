package db.postgresql.protocol.v3.serializers;

import spock.lang.*;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.FixedStream;
import java.nio.charset.Charset;
import java.nio.ByteBuffer;

class ShortSerializerTest extends Specification {
    
    def s = new ShortSerializer();
    def f = Format.TEXT;
    def ascii = Charset.forName('US-ASCII');

    def "Test Length"() {
        expect:
        s.length((short) 0) == 1;
        s.length(Short.MIN_VALUE) == 6;
        s.length(Short.MAX_VALUE) == 5;
        s.length((short) 9_999) == 4;
        s.length((short) 10_000) == 5;
        s.length((short) -9_999) == 5;
        s.length((short) -10_000) == 6;
        s.length((short) 9) == 1;
        s.length((short) 10) == 2;
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
        s.write(fs, (short) 100);
        then:
        toString(fs) == '100';

        when:
        s.write(fs, Short.MAX_VALUE);
        then:
        toString(fs)  == '32767';

        when:
        s.write(fs, (short) 9_999);
        then:
        toString(fs) == '9999';

        when:
        s.write(fs, (short) -10_001);
        then:
        toString(fs) == '-10001';
    }

    def "Test Read"() {
        setup:
        def fs = new FixedStream(20, ascii);

        when:
        strToStream(fs, "9999");
        then:
        s.read(fs, 4) == 9_999;

        when:
        strToStream(fs, '-32768');
        then:
        s.read(fs, 6) == -32768;

        when:
        strToStream(fs, '32767');
        then:
        s.read(fs, 5) == 32767;

        when:
        strToStream(fs, '-12345');
        then:
        s.read(fs, 6) == -12345;
    }
}
