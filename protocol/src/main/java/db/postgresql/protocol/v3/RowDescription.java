package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.PostgresqlStream;
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
    
    public RowDescription(final PostgresqlStream stream, final int size) {
        super(BackEnd.RowDescription, size);
        fields = new FieldDescriptor[0xFFFF & stream.getShort()];
        for(int i = 0; i < fields.length; ++i) {
            fields[i] = new FieldDescriptor(stream);
        }
    }
}
