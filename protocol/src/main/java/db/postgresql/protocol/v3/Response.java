package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.Charset;

public class Response {

    protected ByteBuffer buffer;
    protected Charset encoding;
    
    public BackEnd getBackEnd() {
        return backEnd;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public Charset getEncoding() {
        return encoding;
    }

    protected Response reset(final ByteBuffer buffer, final Charset encoding) {
        this.buffer = buffer;
        this.encoding = encoding;
        return this;
    }

    final protected ByteBuffer freshBuffer() {
        if(buffer == null) {
            return null;
        }
        else {
            ByteBuffer fresh = ByteBuffer.allocate(buffer.remaining());
            fresh.put(buffer);
            return fresh;
        }
    }

    private final BackEnd backEnd;

    protected Response(final BackEnd backEnd) {
        this.backEnd = backEnd;
    }

    protected Response(final Response toCopy) {
        this.backEnd = toCopy.backEnd;
        this.buffer = toCopy.freshBuffer();
    }

    public Response copy() {
        return this;
    }

    public static final byte NULL = (byte) 0;
    
    public int nextNull(final int startAt) {
        for(int i = startAt; i < buffer.limit(); ++i) {
            if(buffer.get(i) == NULL) {
                return i;
            }
        }

        return -1;
    }

    private static final ThreadLocal<byte[]> stringArea = new ThreadLocal<byte[]>() {
            @Override protected byte[] initialValue() {
                return new byte[1024];
            }
        };

    private static final byte[] ensureStringArea(final int size) {
        if(size <= stringArea.get().length) {
            return stringArea.get();
        }
        else {
            stringArea.set(new byte[size]);
            return stringArea.get();
        }
    }
    
    public String nullString(final int startAt, final int nullAt) {
        final int total = nullAt - startAt;
        byte[] bytes = ensureStringArea(total);
        int pos = buffer.position();
        buffer.position(startAt);
        buffer.get(bytes, 0, total);
        buffer.position(pos);
        return new String(bytes, 0, total, encoding);
    }

    public String nullString() {
        final int nullAt = nextNull(buffer.position());
        final int total = nullAt - buffer.position();
        byte[] bytes = ensureStringArea(total);
        buffer.get(bytes, 0, total);
        buffer.position(buffer.position() + 1);
        return new String(bytes, 0, total, encoding);
    }

    private static final Response bindComplete = new Response(BackEnd.BindComplete);
    private static final Response closeComplete = new Response(BackEnd.CloseComplete);
    private static final Response copyDone = new Response(BackEnd.CopyDone);
    private static final Response emptyQueryResponse = new Response(BackEnd.EmptyQueryResponse);
    private static final Response noData = new Response(BackEnd.NoData);
    private static final Response parseComplete = new Response(BackEnd.ParseComplete);
    private static final Response portalSuspended = new Response(BackEnd.PortalSuspended);

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public Response build(final BackEnd backEnd, final int size, final Stream stream) {
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
