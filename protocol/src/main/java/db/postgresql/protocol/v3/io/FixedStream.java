package db.postgresql.protocol.v3.io;

import db.postgresql.protocol.v3.ProtocolException;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;

public class FixedStream implements Stream {

    private final ByteBuffer recvBuffer;
    private final ByteBuffer sendBuffer;
    private final Charset encoding;
    
    public FixedStream(final int size, final Charset encoding) {
        this.recvBuffer = ByteBuffer.allocate(size);
        this.sendBuffer = ByteBuffer.allocate(size);
        this.encoding = encoding;
    }

    public ByteBuffer getRecvBuffer() { return recvBuffer; }
    public ByteBuffer getSendBuffer() { return sendBuffer; }
    
    public void close() { }
    public Charset getEncoding() { return encoding; }

    public void send(final boolean sendAll) { }
    public void recv() { }
    public void recv(final int atLeast) { }
    public void recv(final int atLeast, final int tries) { }
    public void advance(final int size) { recvBuffer.position(recvBuffer.position() + size); }
    
    public Stream put(final ByteBuffer buffer) { sendBuffer.put(buffer); return this; }
    public Stream put(final byte b) { sendBuffer.put(b); return this; }
    public Stream put(final byte[] bytes) { sendBuffer.put(bytes); return this; }
    public Stream putShort(final short s) { sendBuffer.putShort(s); return this; }
    public Stream putInt(final int i) { sendBuffer.putInt(i); return this; }
    public Stream putNull() { sendBuffer.put(NULL); return this; }

    public Stream putCharSequence(final CharSequence seq) {
        return putCharSequence(seq, encoding);
    }

    public Stream putCharSequence(final CharSequence seq, final Charset encoding) {
        final CharBuffer charBuffer = CharBuffer.wrap(seq);
        final CharsetEncoder encoder = encoding.newEncoder();
        encoder.encode(charBuffer, recvBuffer, false);
        encoder.encode(charBuffer, recvBuffer, true);
        return this;
    }
    
    public Stream putString(final String str) {
        return putCharSequence(str);
    }
    
    public ByteBuffer view(final int max) {
        ByteBuffer v = recvBuffer.slice();
        if(v.remaining() > max) {
            v.limit(max);
        }
        
        return v;
    }

    public ByteBuffer getBuffer(final ByteBuffer buffer) {
        buffer.put(recvBuffer);
        return buffer;
    }
    
    public byte get() { return recvBuffer.get(); }
    public byte get(final int tries) { return get(); }
    public byte[] get(final byte[] dst, final int offset, final int length) {
        recvBuffer.get(dst, offset, length);
        return dst;
    }
    
    public byte[] get(byte[] dst, int offset, int length, int tries) {
        return get(dst, offset, length);
    }
    
    public byte[] get(byte[] dst) {
        return get(dst, 0, dst.length);
    }
    
    public byte[] get(byte[] dst, int tries) {
        return get(dst);
    }

    public String nullString() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
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
        byte[] bytes = get(new byte[size]);
        int length = (bytes[bytes.length - 1] == (byte) 0) ? bytes.length - 1 : bytes.length;
        return new String(bytes, 0, length, encoding);
    }

    public String nullString(final int size, final int tries) {
        return nullString(size);
    }

    public short getShort() { return recvBuffer.getShort(); }
    public short getShort(int tries) { return getShort(); }
    public int getInt() { return recvBuffer.getInt(); }
    public int getInt(final int tries) { return getInt(); }
    public byte getNull() { return get(); }
    public byte getNull(int tries) { return get(); }
}
