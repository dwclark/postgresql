package db.postgresql.protocol.v3.io;

import spock.lang.*;
import java.nio.*;
import java.nio.charset.*;

class BindingStreamTest extends Specification {

    def "Test Basic"() {
        setup:
        BindingStream bstream = new BindingStream(3, Charset.forName("UTF-8"));
        bstream.begin(); bstream.put((byte) 10); bstream.end();
        bstream.begin(); bstream.putShort((short) 100); bstream.end();
        bstream.begin(); bstream.putInt(1000); bstream.end();
        List fields = bstream.fields;
        
        expect:
        fields[0].size() == 1;
        fields[0].buffer().remaining() == fields[0].size();
        fields[0].buffer().get() == 10;

        fields[1].size() == 2;
        fields[1].buffer().remaining() == fields[1].size();
        fields[1].buffer().getShort() == 100;

        fields[2].size() == 4;
        fields[2].buffer().remaining() == fields[2].size();
        fields[2].buffer().getInt() == 1000;
    }
}
