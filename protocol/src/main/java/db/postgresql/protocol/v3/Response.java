package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.Charset;

public class Response {

    public BackEnd getBackEnd() {
        return backEnd;
    }

    private final BackEnd backEnd;

    protected Response(final BackEnd backEnd) {
        this.backEnd = backEnd;
    }

    public static final byte NULL = (byte) 0;

    public boolean isNull(final int val) {
        return val == -1;
    }
    
    private static final Response bindComplete = new Response(BackEnd.BindComplete);
    private static final Response closeComplete = new Response(BackEnd.CloseComplete);
    private static final Response copyDone = new Response(BackEnd.CopyDone);
    private static final Response emptyQueryResponse = new Response(BackEnd.EmptyQueryResponse);
    private static final Response noData = new Response(BackEnd.NoData);
    private static final Response parseComplete = new Response(BackEnd.ParseComplete);
    private static final Response portalSuspended = new Response(BackEnd.PortalSuspended);

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public Response build(final BackEnd backEnd, final int size, final PostgresqlStream stream) {
                switch(backEnd) {
                case BindComplete: return bindComplete;
                case CloseComplete: return closeComplete;
                case CopyDone: return copyDone;
                case EmptyQueryResponse: return emptyQueryResponse;
                case NoData: return noData;
                case ParseComplete: return parseComplete;
                case PortalSuspended: return portalSuspended;
                default: throw new IllegalArgumentException("" + backEnd + " not supported");
                }
            }
        };
}
