package db.postgresql.protocol.v3;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

public class FrontEndFormatter {

    public static final Charset charset = Charset.forName("UTF-8");

    public ByteBuffer[] startup(final String user, final String[] keys, final String[] values) {
        final BufferChain chain = new BufferChain()
            .sizeHere()
            .putInt(FrontEnd.StartupMessage.code)
            .putNullString("user", charset)
            .putNullString(user, charset);
        
        if(keys != null && values != null) {
            assert(keys.length == values.length);
            for(int i = 0; i < keys.length; ++i) {
                chain.putNullString(keys[i], charset).putNullString(values[i], charset);
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
}
