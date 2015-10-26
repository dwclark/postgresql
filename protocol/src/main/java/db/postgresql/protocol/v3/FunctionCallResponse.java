package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.PostgresqlStream;

public class FunctionCallResponse extends Response {

    final private int length;
    final private byte[] data;

    public int getLength() {
        return length;
    }

    public byte[] getData() {
        return data;
    }
    
    public FunctionCallResponse(final PostgresqlStream stream, final int size) {
        super(BackEnd.FunctionCallResponse, size);
        length = stream.getInt();
        if(length > 0) {
            data = stream.get(new byte[length]);
        }
    }
}
