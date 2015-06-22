package db.postgresql.protocol.v3.io;

import java.nio.ByteBuffer;
import db.postgresql.protocol.v3.ProtocolException;

public class Stream {

    public static final byte NULL = (byte) 0;
    
    private final ByteBuffer sendBuffer;
    private final ByteBuffer recvBuffer;
    private final IO io;
    
    public Stream(IO io) {
        this.io = io;
        int size = Math.max(io.getAppMinBufferSize(), 65_536);
        this.sendBuffer = ByteBuffer.allocate(size);
        this.recvBuffer = ByteBuffer.allocate(size);
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

    private void ensureForRecv(final int size) {
        if(size > recvBuffer.capacity()) {
             String msg = String.format("Stream can only hold %d bytes, " +
                                       "request payload into pieces no larger than this", recvBuffer.capacity());
            throw new ProtocolException(msg);
        }

        while(size > (recvBuffer.capacity() - recvBuffer.limit())) {
            recv();
        }
    }

    public ByteBuffer getBuffer(ByteBuffer buffer) {
        ensureForRecv(buffer.remaining());
        buffer.put(recvBuffer);
        return buffer;
    }

    public byte get() {
        ensureForRecv(1);
        return recvBuffer.get();
    }

    public byte[] get(byte[] dst, int offset, int length) {
        ensureForRecv(length);
        sendBuffer.put(dst, offset, length);
        return dst;
    }

    public short getShort() {
        ensureForRecv(2);
        return sendBuffer.getShort();
    }

    public int getInt() {
        ensureForRecv(4);
        return sendBuffer.getInt();
    }

    public byte getNull() {
        return get();
    }
}
