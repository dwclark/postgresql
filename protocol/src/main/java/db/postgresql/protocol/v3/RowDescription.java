package db.postgresql.protocol.v3;

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
    
    private RowDescription(final FieldDescriptor[] fields) {
        super(BackEnd.RowDescription);
        this.fields = fields;
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public RowDescription build(final BackEnd backEnd, final int size, final PostgresqlStream stream) {
                FieldDescriptor[] fields = new FieldDescriptor[0xFFFF & stream.getShort()];
                for(int i = 0; i < fields.length; ++i) {
                    fields[i] = new FieldDescriptor(stream);
                }
                
                return new RowDescription(fields);
            }
        };
}
