package db.postgresql.protocol.v3.serializers;

import spock.lang.*;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.FixedStream;
import java.nio.charset.Charset;
import java.nio.ByteBuffer;

class ShortSerializerTest extends Specification {
    
    def s = ShortSerializer.instance;
    def f = Format.TEXT;
    def ascii = Charset.forName('US-ASCII');

    def "Test Length"() {
        expect:
        s.length((short) 0, f) == 1;
        s.length(Short.MIN_VALUE, f) == 6;
        s.length(Short.MAX_VALUE, f) == 5;
        s.length((short) 9_999, f) == 4;
        s.length((short) 10_000, f) == 5;
        s.length((short) -9_999, f) == 5;
        s.length((short) -10_000, f) == 6;
        s.length((short) 9, f) == 1;
        s.length((short) 10, f) == 2;
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
        s.write(fs, (short) 100, f);
        then:
        toString(fs) == '100';

        when:
        s.write(fs, Short.MAX_VALUE, f);
        then:
        toString(fs)  == '32767';

        when:
        s.write(fs, (short) 9_999, f);
        then:
        toString(fs) == '9999';

        when:
        s.write(fs, (short) -10_001, f);
        then:
        toString(fs) == '-10001';
    }

    def "Test Read"() {
        setup:
        def fs = new FixedStream(20, ascii);

        when:
        strToStream(fs, "9999");
        then:
        s.read(fs, 4, f) == 9_999;

        when:
        strToStream(fs, '-32768');
        then:
        s.read(fs, 6, f) == -32768;

        when:
        strToStream(fs, '32767');
        then:
        s.read(fs, 5, f) == 32767;

        when:
        strToStream(fs, '-12345');
        then:
        s.read(fs, 6, f) == -12345;
    }
}
