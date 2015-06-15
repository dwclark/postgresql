package db.postgresql.protocol.v3;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.nio.charset.Charset;
import static db.postgresql.protocol.v3.Formatter.NULL;

public class BufferChain {
    
    private static final int BLOCK_SIZE = 1024;

    private final List<ByteBuffer> chain;
    private int _current;
    private int sizeAt = 0;
    private final byte[] intBuffer = new byte[4];
    private final ByteBuffer conversions = ByteBuffer.wrap(intBuffer);

    public BufferChain() {
        chain = new ArrayList<>();
        chain.add(ByteBuffer.allocate(BLOCK_SIZE));
        _current = 0;
    }

    public BufferChain(ByteBuffer initial) {
        chain = new ArrayList<>();
        chain.add(initial);
        _current = 0;
    }

    private ByteBuffer current() {
        return chain.get(_current);
    }

    private ByteBuffer next() {
        return chain.get(++_current);
    }

    private void ensure(final int size) {
        ByteBuffer buffer = chain.get(_current);
        if(buffer.remaining() < size) {
            int num = numBuffers(size);
            for(int i = 0; i < num; ++i) {
                chain.add(ByteBuffer.allocate(BLOCK_SIZE));
            }
        }
    }

    private int numBuffers(final int size) {
        int stillNeed = size - current().remaining();
        return ((stillNeed / BLOCK_SIZE) +
                ((stillNeed % BLOCK_SIZE > 0) ? 1 : 0));
    }

    public BufferChain putByteBuffer(ByteBuffer buffer) {
        chain.add(buffer);
        ++_current;
        return this;
    }
    
    public BufferChain put(final byte b) {
        intBuffer[0] = b;
        writeBytes(intBuffer, 0, 1);
        return this;
    }

    public BufferChain putNull() {
        put(NULL);
        return this;
    }
    
    public BufferChain putShort(final short s) {
        conversions.putShort(s).flip();
        writeBytes(intBuffer, 0, 2);
        return this;
    }

    public BufferChain putInt(final int i) {
        conversions.putInt(i).flip();
        writeBytes(intBuffer, 0, 4);
        return this;
    }

    public BufferChain sizeHere() {
        sizeAt = current().position();
        putInt(0);
        return this;
    }

    public BufferChain putBytes(final byte[] bytes) {
        writeBytes(bytes, 0, bytes.length);
        return this;
    }

    private void writeBytes(final byte[] bytes, final int pos, int total) {
        ensure(total);
        int offset = pos;
        int left = total;

        while(left != 0) {
            ByteBuffer buffer = current();
            if(left <= buffer.remaining()) {
                buffer.put(bytes, offset, left);
                left = 0;
            }
            else {
                int toWrite = Math.min(left, buffer.remaining());
                if(toWrite > 0) {
                    buffer.put(bytes, offset, toWrite);
                    offset += toWrite;
                    left -= toWrite;
                }
                
                next();
            }
        }
    }

    public BufferChain putNullString(final String str, final Charset charset) {
        byte[] ary = str.getBytes(charset);
        writeBytes(ary, 0, ary.length);
        putNull();
        return this;
    }

    public ByteBuffer[] forWrite() {
        ByteBuffer[] ret = new ByteBuffer[chain.size()];
        int total = 0;
        for(int i = 0; i < chain.size(); ++i) {
            ret[i] = (ByteBuffer) chain.get(i).flip();
            total += ret[i].remaining();
        }

        ret[0].putInt(sizeAt, total - sizeAt);
        return ret;
    }
}
