package db.postgresql.protocol.v3.io;

import db.postgresql.protocol.v3.ProtocolException;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;

public class NetworkStream implements Stream {

    public static final int DEFAULT_TRIES = 5;
    
    private final ByteBuffer sendBuffer;
    private ByteBuffer recvBuffer;
    private final IO io;
    private final Charset encoding;
    private final ByteBuffer fixedSizeOps = ByteBuffer.allocate(8);
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
    
    public NetworkStream(final IO io, final Charset encoding) {
        this.io = io;
        int size = Math.max(io.getAppMinBufferSize(), 65_536);
        this.sendBuffer = ByteBuffer.allocate(size);
        this.recvBuffer = ByteBuffer.allocate(size);
        this.encoding = encoding;

        //position the recvBuffer at the end so that the first attempt
        //to recv shows that there isn't anything there.
        recvBuffer.position(recvBuffer.limit());
    }

    public void wakeup() {
        io.wakeup();
    }

    public void close() {
        io.close();
    }

    public ByteBuffer getRecvBuffer() {
        return recvBuffer;
    }

    public Charset getEncoding() {
        return encoding;
    }

    public void send(final boolean sendAll) {
        sendBuffer.flip();
        io.write(sendBuffer);

        if(sendAll) {
            while(sendBuffer.hasRemaining()) {
                io.write(sendBuffer);
            }
        }
        
        sendBuffer.compact();
    }

    public void recv() {
        recv(0, 1);
    }
    
    public void recv(final int atLeast) {
        recv(atLeast, DEFAULT_TRIES);
    }

    public void recv(final int atLeast, final int tries) {
        recvBuffer.compact();
        io.read(recvBuffer);
        int passes = 1;
        while((recvBuffer.position() < atLeast) && (passes < tries)) {
            io.read(recvBuffer);
        }
        recvBuffer.flip();
    }

    public void advance(final int size) {
        int left = safeAdvance(size);
        while(left != 0) {
            recv();
            left = safeAdvance(left);
        }
    }

    private int safeAdvance(final int left) {
        final int advanceBy = Math.min(left, recvBuffer.remaining());
        recvBuffer.position(recvBuffer.position() + advanceBy);
        return left - advanceBy;
    }

    public Stream put(final ByteBuffer buffer) {
        while(buffer.hasRemaining()) {
            final int originalLimit = buffer.limit();
            final int newLimit = Math.min(sendBuffer.remaining(), buffer.remaining());
            buffer.limit(buffer.position() + newLimit);
            sendBuffer.put(buffer);
            buffer.limit(originalLimit);
            
            if(buffer.hasRemaining()) {
                send(false);
            }
        }

        return this;
    }

    public Stream put(final byte b) {
        if(sendBuffer.remaining() >= 1) {
            sendBuffer.put(b);
        }
        else {
            fixedSizeOps.clear();
            fixedSizeOps.put(b);
            fixedSizeOps.flip();
            put(fixedSizeOps);
        }
        
        return this;
    }

    public Stream put(final byte[] bytes) {
        put(ByteBuffer.wrap(bytes));
        return this;
    }

    public Stream putShort(final short s) {
        if(sendBuffer.remaining() >= 2) {
            sendBuffer.putShort(s);
        }
        else {
            fixedSizeOps.clear();
            fixedSizeOps.putShort(s);
            fixedSizeOps.flip();
            put(fixedSizeOps);
        }
        return this;
    }

    public Stream putInt(final int i) {
        if(sendBuffer.remaining() >= 4) {
            sendBuffer.putInt(i);
        }
        else {
            fixedSizeOps.clear();
            fixedSizeOps.putInt(i);
            fixedSizeOps.flip();
            put(fixedSizeOps);
        }
        
        return this;
    }

    public Stream putNull() {
        put((byte) 0);
        return this;
    }

    public Stream putCharSequence(final CharSequence seq) {
        return putCharSequence(seq, encoding);
    }

    public Stream putCharSequence(final CharSequence seq, final Charset encoding) {
        //we don't need to ensure for this one. If we can't
        //send it in one shot, then we can split it into multiple sends
        final CharBuffer charBuffer = CharBuffer.wrap(seq);
        final CharsetEncoder encoder = encoding.newEncoder();
        
        while(encoder.encode(charBuffer, sendBuffer, false) == CoderResult.OVERFLOW) {
            send(false);
        }

        while(encoder.encode(charBuffer, sendBuffer, true) == CoderResult.OVERFLOW) {
            send(false);
        }

        return this;
    }

    public Stream putString(final String str) {
        return putCharSequence(str);
    }
    
    protected void ensureForRecv(final int size) {
        ensureForRecv(size, DEFAULT_TRIES);
    }

    protected void ensureForRecv(final int size, final int tries) {
        if(size <= recvBuffer.remaining()) {
            return;
        }
        
        //we need to ensure we have at least remaining + size space available
        //recv will compact already used space, so we can ignore anything before position
        if(recvBuffer.capacity() < (recvBuffer.remaining() + size)) {
            ByteBuffer newRecvBuffer = ByteBuffer.allocate(recvBuffer.remaining() + size);
            newRecvBuffer.put(recvBuffer);
            recvBuffer = newRecvBuffer;
        }

        recv(size, tries);

        if(recvBuffer.remaining() < size) {
            NoData.now();
        }
    }

    public ByteBuffer view(final int max) {
        ByteBuffer v = recvBuffer.slice();
        if(v.remaining() > max) {
            v.limit(max);
        }

        return v;
    }

    public ByteBuffer getBuffer(final ByteBuffer buffer) {
        return getBuffer(buffer, DEFAULT_TRIES);
    }

    public ByteBuffer getBuffer(final ByteBuffer buffer, final int tries) {
        ensureForRecv(buffer.remaining(), tries);
        buffer.put(recvBuffer);
        return buffer;
    }

    public byte get() {
        return get(DEFAULT_TRIES);
    }

    public byte get(final int tries) {
        ensureForRecv(1, tries);
        return recvBuffer.get();
    }

    public byte[] get(final byte[] dst, final int offset, final int length) {
        return get(dst, offset, length, DEFAULT_TRIES);
    }

    public byte[] get(final byte[] dst, final int offset, final int length, final int tries) {
        ensureForRecv(length, tries);
        recvBuffer.get(dst, offset, length);
        return dst;
    }

    public byte[] get(final byte[] dst) {
        return get(dst, 0, dst.length, DEFAULT_TRIES);
    }

    public byte[] get(final byte[] dst, final int tries) {
        return get(dst, 0, 0, tries);
    }

    public String nullString() {
        try {
            baos.reset();
            byte b;
            while((b = get()) != NULL) {
                baos.write(b);
            }
            
            return baos.toString(encoding.name());
        }
        catch(UnsupportedEncodingException ex) {
            throw new ProtocolException(ex);
        }
    }

    public String nullString(final int size) {
        return nullString(size, DEFAULT_TRIES);
    }

    public String nullString(final int size, final int tries) {
        byte[] bytes = get(new byte[size], tries);
        int length = (bytes[bytes.length - 1] == (byte) 0) ? bytes.length - 1 : bytes.length;
        return new String(bytes, 0, length, encoding);
    }

    public short getShort() {
        return getShort(DEFAULT_TRIES);
    }

    public short getShort(final int tries) {
        ensureForRecv(2, tries);
        return recvBuffer.getShort();
    }

    public int getInt() {
        return getInt(DEFAULT_TRIES);
    }

    public int getInt(final int tries) {
        ensureForRecv(4, tries);
        return recvBuffer.getInt();
    }

    public byte getNull() {
        return get();
    }

    public byte getNull(final int tries) {
        return get(tries);
    }
}
