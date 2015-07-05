package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Arrays;
import java.nio.charset.Charset;

public class RowDescription extends Response {

    private final FieldDescriptor[] fields;

    public FieldDescriptor field(final int i) {
        return fields[i];
    }
    
    private RowDescription(final Stream stream, final int num) {
        super(BackEnd.RowDescription);
        this.fields = new FieldDescriptor[num];
        for(int i = 0; i < num; ++i) {
            this.fields[i] = new FieldDescriptor(stream);
        }
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public RowDescription build(final BackEnd backEnd, final int size, final Stream stream) {
                return new RowDescription(stream, 0xFFFF & stream.getShort());
            }
        };
}
