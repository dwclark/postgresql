package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Response {

    public BackEnd getBackEnd() {
        return backEnd;
    }

    private final BackEnd backEnd;

    protected Response(final BackEnd backEnd) {
        this.backEnd = backEnd;
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public Response build(final BackEnd backEnd, final int size, final Stream stream) {
                stream.advance(size);
                return new Response(backEnd);
            }
        };

}
