package db.postgresql.protocol.v3;

import java.nio.charset.Charset;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.AbstractMap;

public class Formatter {

    public static final byte NULL = (byte) 0;
    private final Charset charset;

    public Charset getCharset() {
        return charset;
    }

    public Formatter(final Charset charset) {
        this.charset = charset;
    }

    public ByteBuffer[] startup(final Map<String,String> keysValues) {
        assert(keysValues != null && !keysValues.isEmpty());
        
        final BufferChain chain = new BufferChain()
            .sizeHere()
            .putInt(FrontEnd.StartupMessage.code);
        
        if(keysValues != null) {
            for(Map.Entry<String,String> entry : keysValues.entrySet()) {
                chain.putNullString(entry.getKey(), charset).putNullString(entry.getValue(), charset);
            }
        }

        return chain.putNull().forWrite();
    }

    public ByteBuffer cancel(final int pid, final int key) {
        return (ByteBuffer) ByteBuffer.allocate(16)
            .putInt(16)
            .putInt(FrontEnd.CancelRequest.code)
            .putInt(pid)
            .putInt(key)
            .flip();
    }

    private ByteBuffer[] close(final char type, final String name) {
        return new BufferChain()
            .put(FrontEnd.Close.toByte())
            .sizeHere()
            .put((byte) type)
            .putNullString(name, charset).forWrite();
    }

    public ByteBuffer[] closeStatement(final String name) {
        return close('S', name);
    }

    public ByteBuffer[] closePortal(final String name) {
        return close('P', name);
    }

    public ByteBuffer[] copyData(final List<ByteBuffer> buffers) {
        final BufferChain chain = new BufferChain(ByteBuffer.allocate(5))
            .put(FrontEnd.CopyData.toByte())
            .sizeHere();
        
        for(ByteBuffer buffer : buffers) {
            chain.putByteBuffer(buffer);
        }
        
        return chain.forWrite();
    }

    public ByteBuffer copyDone() {
        return (ByteBuffer) ByteBuffer.allocate(5)
            .put(FrontEnd.CopyDone.toByte())
            .putInt(4)
            .flip();
    }

    public ByteBuffer[] copyFail(final String message) {
        return new BufferChain()
            .put(FrontEnd.CopyFail.toByte())
            .sizeHere()
            .putNullString(message, charset)
            .forWrite();
    }

    private ByteBuffer[] describe(final char type, final String name) {
        return new BufferChain()
            .put(FrontEnd.Describe.toByte())
            .sizeHere()
            .put((byte) type)
            .putNullString(name, charset)
            .forWrite();
    }

    public ByteBuffer[] describeStatement(final String name) {
        return describe('S', name);
    }

    public ByteBuffer[] describePortal(final String name) {
        return describe('P', name);
    }

    public ByteBuffer[] execute(final String name) {
        return execute(name, 0);
    }

    public ByteBuffer[] execute(final String name, final int maxRows) {
        return new BufferChain()
            .put(FrontEnd.Execute.toByte())
            .sizeHere()
            .putNullString(name, charset)
            .putInt(maxRows)
            .forWrite();
    }

    public ByteBuffer flush() {
        return (ByteBuffer) ByteBuffer.allocate(5)
            .put(FrontEnd.Flush.toByte())
            .putInt(4)
            .flip();
    }

    public ByteBuffer[] password(String password) {
        return password(password.getBytes(charset));
    }

    public ByteBuffer[] password(byte[] password) {
        return new BufferChain()
            .put(FrontEnd.Password.toByte())
            .sizeHere()
            .putBytes(password)
            .forWrite();
    }

    public ByteBuffer[] query(String str) {
        return new BufferChain()
            .put(FrontEnd.Query.toByte())
            .sizeHere()
            .putNullString(str, charset)
            .forWrite();
    }

    public ByteBuffer ssl() {
        return (ByteBuffer) ByteBuffer.allocate(8)
            .putInt(8)
            .putInt(FrontEnd.SSLRequest.code)
            .flip();
    }

    public ByteBuffer sync() {
        return (ByteBuffer) ByteBuffer.allocate(5)
            .put(FrontEnd.Sync.toByte())
            .putInt(4)
            .flip();
    }

    public ByteBuffer terminate() {
        return (ByteBuffer) ByteBuffer.allocate(5)
            .put(FrontEnd.Terminate.toByte())
            .putInt(4)
            .flip();
    }

    public String nullString(ByteBuffer buffer, int size) {
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        if(bytes[bytes.length-1] == NULL) {
            return new String(bytes, 0, bytes.length - 1, charset);
        }
        else {
            return new String(bytes, charset);
        }
    }

    public Map.Entry<String,String> nullPair(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        
        int nullPos;
        for(nullPos = 0; nullPos < bytes.length; ++nullPos) {
            if(bytes[nullPos] == NULL) {
                break;
            }
        }

        return new AbstractMap.SimpleImmutableEntry(new String(bytes, 0, nullPos, charset),
                                                    new String(bytes, nullPos + 1, ((bytes.length - nullPos) - 2), charset));
    }
}
