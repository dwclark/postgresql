package db.postgresql.protocol.v3;

import db.postresql.protocol.v3.io.Stream;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class Response {

    public BackEnd getBackEnd() {
        return backEnd;
    }

    private final BackEnd backEnd;

    protected Response(final BackEnd backEnd) {
        this.backEnd = backEnd;
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public Response read(BackEnd backEnd, int size, Stream stream) {
                stream.advance(size);
                return new Response(backEnd);
            }
        };
}
