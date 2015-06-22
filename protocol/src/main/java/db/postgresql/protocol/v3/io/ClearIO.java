package db.postgresql.protocol.v3.io;

import db.postgresql.protocol.v3.ProtocolException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.util.Iterator;
import java.io.IOException;
import java.io.EOFException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

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
            toRead.flip();
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
            int numBytes = channel.read(buffer);
            if(numBytes == -1) {
                throw new EOFException("Reading on closed channel");
            }
        }

        if(key.isWritable()) {
            int numBytes = channel.write(buffer);
            if(numBytes == -1) {
                throw new EOFException("Writing on closed channel");
            }
        }
    }
}
