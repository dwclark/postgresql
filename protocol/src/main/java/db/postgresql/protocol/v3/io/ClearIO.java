package db.postgresql.protocol.v3.io;

import db.postgresql.protocol.v3.ProtocolException;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class ClearIO extends IO {

    private SocketChannel channel;
    private Selector selector;

    public ClearIO(final String host, final int port) {
        super(host, port);
        
        try {
            this.channel = SocketChannel.open();
            this.selector = Selector.open();
            channel.configureBlocking(true);
            channel.connect(new InetSocketAddress(InetAddress.getByName(host), port));
            channel.configureBlocking(false);
            channel.register(selector, 0);
        }
        catch(IOException ex) {
            throw new ProtocolException(ex);
        }
    }

    public int getAppMinBufferSize() {
        return 8192;
    }
    
    public void write(ByteBuffer toWrite) {
        try {
            io(SelectionKey.OP_WRITE, toWrite);
        }
        catch(IOException ex) {
            throw new ProtocolException(ex);
        }
    }
    
    public void read(ByteBuffer toRead) {
        try {
            io(SelectionKey.OP_READ, toRead);
        }
        catch(IOException ex) {
            throw new ProtocolException(ex);
        }
    }

    private void io(int ops, ByteBuffer buffer) throws IOException {
        channel.register(selector, ops);
        int count = selector.select(30_000);
        Iterator iter = selector.selectedKeys().iterator();
        if(!iter.hasNext()) {
            return;
        }

        SelectionKey key = (SelectionKey) iter.next();
        iter.remove();
        if(!key.isValid()) {
            return;
        }

        if(key.isReadable()) {
            checkEof(channel.read(buffer));
        }

        if(key.isWritable()) {
            checkEof(channel.write(buffer));
        }
    }

    private void checkEof(int numBytes) throws EOFException {
        if(numBytes == -1) {
            throw new EOFException();
        }
    }

    public void close() {
        try {
            selector.close();
            channel.close();
        }
        catch(IOException ex) {
            throw new ProtocolException(ex);
        }
    }
}
