package db.postgresql.protocol.v3.io;

import java.nio.ByteBuffer;
import db.postgresql.protocol.v3.ProtocolException;
import java.nio.charset.Charset;

public class Stream {

    public static final byte NULL = (byte) 0;
    
    private final ByteBuffer sendBuffer;
    private final ByteBuffer recvBuffer;
    private final IO io;
    private final Charset encoding;
    
    public Stream(IO io, Charset encoding) {
        this.io = io;
        int size = Math.max(io.getAppMinBufferSize(), 65_536);
        this.sendBuffer = ByteBuffer.allocate(size);
        this.recvBuffer = ByteBuffer.allocate(size);
        this.encoding = encoding;
    }

    public ByteBuffer getRecvBuffer() {
        return recvBuffer;
    }

    public Charset getEncoding() {
        return encoding;
    }

    public void send() {
        sendBuffer.flip();
        io.write(sendBuffer);
        sendBuffer.compact();
    }

    public void sendAll() {
        while(sendBuffer.hasRemaining()) {
            send();
        }
    }

    public void recv() {
        recvBuffer.compact();
        io.read(recvBuffer);
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

    private void ensureForSend(final int size) {
        if(size > sendBuffer.capacity()) {
            String msg = String.format("Stream can only hold %d bytes, " +
                                       "break up payload into pieces no larger than this", sendBuffer.capacity());
            throw new ProtocolException(msg);
        }

        while(size > sendBuffer.remaining()) {
            send();
        }
    }

    public Stream put(ByteBuffer buffer) {
        ensureForSend(buffer.remaining());
        sendBuffer.put(buffer);
        return this;
    }

    public Stream put(byte b) {
        ensureForSend(1);
        sendBuffer.put(b);
        return this;
    }

    public Stream put(byte[] bytes) {
        ensureForSend(bytes.length);
        sendBuffer.put(bytes);
        return this;
    }

    public Stream putShort(short s) {
        ensureForSend(2);
        sendBuffer.putShort(s);
        return this;
    }

    public Stream putInt(int i) {
        ensureForSend(4);
        sendBuffer.putInt(i);
        return this;
    }

    public Stream putNull() {
        put((byte) 0);
        return this;
    }

    protected void ensureForRecv(final int size) {
        ensureForRecv(size, Integer.MAX_VALUE);
    }

    private boolean enoughInRecvBuffer(final int size) {
        return size >= (recvBuffer.capacity() - recvBuffer.limit());
    }
    
    protected void ensureForRecv(final int size, final int tries) {
        if(size > recvBuffer.capacity()) {
             String msg = String.format("Stream can only hold %d bytes, " +
                                       "request payload into pieces no larger than this", recvBuffer.capacity());
            throw new ProtocolException(msg);
        }

        int passes = 0;
        while(!enoughInRecvBuffer(size) && (passes++ < tries)) {
            recv();
        }

        if(!enoughInRecvBuffer(size)) {
            NoData.now();
        }
    }

    public ByteBuffer getBuffer(final ByteBuffer buffer) {
        return getBuffer(buffer, Integer.MAX_VALUE);
    }

    public ByteBuffer getBuffer(final ByteBuffer buffer, final int tries) {
        ensureForRecv(buffer.remaining(), tries);
        buffer.put(recvBuffer);
        return buffer;
    }

    public byte get() {
        return get(Integer.MAX_VALUE);
    }

    public byte get(final int tries) {
        ensureForRecv(1, tries);
        return recvBuffer.get();
    }

    public byte[] get(final byte[] dst, final int offset, final int length) {
        return get(dst, offset, length, Integer.MAX_VALUE);
    }

    public byte[] get(final byte[] dst, final int offset, final int length, final int tries) {
        ensureForRecv(length, tries);
        recvBuffer.get(dst, offset, length);
        return dst;
    }

    public byte[] get(final byte[] dst) {
        return get(dst, 0, 0, Integer.MAX_VALUE);
    }

    public byte[] get(final byte[] dst, final int tries) {
        return get(dst, 0, 0, tries);
    }

    public String nullString(final int size) {
        return nullString(size, Integer.MAX_VALUE);
    }

    public String nullString(final int size, final int tries) {
        byte[] bytes = get(new byte[size], tries);
        int length = (bytes[bytes.length - 1] == (byte) 0) ? bytes.length - 1 : bytes.length;
        return new String(bytes, 0, length, encoding);
    }

    public short getShort() {
        return getShort(Integer.MAX_VALUE);
    }

    public short getShort(final int tries) {
        ensureForRecv(2, tries);
        return recvBuffer.getShort();
    }

    public int getInt() {
        return getInt(Integer.MAX_VALUE);
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
