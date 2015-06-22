package db.postgresql.protocol.v3.io;

import db.postgresql.protocol.v3.ProtocolException;
import java.io.IOException;
import java.io.EOFException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;
import java.nio.BufferOverflowException;
import java.nio.channels.SocketChannel;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.util.Iterator;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

public class SslIO implements IO {

    //truly private
    private final SSLEngine engine;
    private final ByteBuffer recvBuffer;
    private final ByteBuffer sendBuffer;
    private final SocketChannel channel;
    private final Selector selector;

    //expose as properties
    private final String host;
    private final int port;
    private final int appMinBufferSize;
    private final int netMinBufferSize;

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getAppMinBufferSize() {
        return appMinBufferSize;
    }

    public int getNetMinBufferSize() {
        return netMinBufferSize;
    }

    public SslIO(final String host, final int port, final SSLContext context) {
        this.host = host;
        this.port = port;

        try {
            //Start with the channel in blocking mode, the application is going to have
            //to block initially while the ssl layer starts up.  Might as well
            //get the JVM and the OS to do the blocking for us as needed.
            channel = SocketChannel.open();
            channel.configureBlocking(true);
            channel.connect(new InetSocketAddress(InetAddress.getByName(host), port));
            
            this.engine = context.createSSLEngine(host, port);
            engine.setUseClientMode(true);
            engine.setWantClientAuth(false);
            this.selector = Selector.open();
            
            this.appMinBufferSize = engine.getSession().getApplicationBufferSize();
            this.netMinBufferSize = engine.getSession().getPacketBufferSize();
            this.recvBuffer = netBuffer();
            this.sendBuffer = netBuffer();
            
            engine.beginHandshake();
            ssl();

            //at this point ssl is going, reset the channel to async and add it to the selector
            channel.configureBlocking(false);
            channel.register(selector, 0);
        }
        catch(IOException ioe) {
            throw new ProtocolException(ioe);
        }
    }

    public void write(final ByteBuffer toWrite) {
        try {
            if(sendBuffer.hasRemaining()) {
                io(SelectionKey.OP_WRITE);
            }
            
            SSLEngineResult result = null;
            while(toWrite.hasRemaining() &&
                  (result = engine.wrap(toWrite, sendBuffer)).getStatus() == SSLEngineResult.Status.OK) { }
            
            if(result.getStatus() == SSLEngineResult.Status.OK ||
               result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                io(SelectionKey.OP_WRITE);
            }
            else if(result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                throw new BufferUnderflowException();
            }
            else if(result.getStatus() == SSLEngineResult.Status.CLOSED) {
                throw new EOFException();
            }
        }
        catch(IOException ex) {
            throw new ProtocolException(ex);
        }

        //TODO: need to handle re-handshake at this point

    }

    public void read(final ByteBuffer toRead) {
        assert(toRead.remaining() >= appMinBufferSize);

        try {
            io(SelectionKey.OP_READ);
            recvBuffer.flip();
            SSLEngineResult result = null;
            while(recvBuffer.hasRemaining() &&
                  (result = engine.unwrap(recvBuffer, toRead)).getStatus() == SSLEngineResult.Status.OK) { }
            
            recvBuffer.compact();
            toRead.flip();

            //ignore buffer over flow, the caller should clear out their buffer and if
            //not then the assert at the beginning should catch that problem
            if(result.getStatus() == SSLEngineResult.Status.CLOSED) {
                throw new EOFException();
            }

        }
        catch(IOException ex) {
            throw new ProtocolException(ex);
        }
        //TODO: Need to handle re-handshake at this point

    }

    private ByteBuffer netBuffer() {
        return ByteBuffer.allocate(netMinBufferSize);
    }

    private boolean needHandshake(SSLEngineResult.HandshakeStatus status) {
        return !(status == SSLEngineResult.HandshakeStatus.FINISHED ||
                 status == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING);
    }

    private void io(int ops) throws IOException {
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
            int numBytes = channel.read(recvBuffer);
            if(numBytes == -1) {
                throw new EOFException("Reading on closed channel");
            }
        }

        if(key.isWritable()) {
            sendBuffer.flip();
            int numBytes = channel.write(sendBuffer);
            if(numBytes == -1) {
                throw new EOFException("Writing on closed channel");
            }
            
            sendBuffer.compact();
        }
    }

    
    //SSL Methods below this point
    private void ssl() {
        ByteBuffer appSendBuffer = ByteBuffer.allocate(0);
        ByteBuffer appRecvBuffer = ByteBuffer.allocate(appMinBufferSize);
        
        try {
            SSLEngineResult.HandshakeStatus hstatus = engine.getHandshakeStatus();
            while(needHandshake(hstatus)) {
                if(hstatus == SSLEngineResult.HandshakeStatus.NEED_TASK) {
                    hstatus = sslTasks();
                }
                else if(hstatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
                    hstatus = sslUnwrap(appRecvBuffer, recvBuffer);
                }
                else if(hstatus == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
                    hstatus = sslWrap(appSendBuffer, sendBuffer);
                }
            }
            
            sendBuffer.clear(); recvBuffer.clear();
        }
        catch(IOException ex) {
            throw new ProtocolException(ex);
        }
    }

    private void assertNotEof(final int num) throws EOFException {
        if(num == -1) {
            throw new EOFException();
        }
    }
    
    private SSLEngineResult.HandshakeStatus sslWrap(final ByteBuffer app, final ByteBuffer net)
        throws IOException {
        app.clear(); net.clear();
        SSLEngineResult result = engine.wrap(app, net);
        
        net.flip();
        while(net.hasRemaining()) {
            assertNotEof(channel.write(net));
        }

        return result.getHandshakeStatus();
    }

    private SSLEngineResult.HandshakeStatus sslUnwrap(final ByteBuffer app, final ByteBuffer net)
        throws IOException {
        app.clear(); net.clear();
        assertNotEof(channel.read(net));
        net.flip();
        
        SSLEngineResult result = engine.unwrap(net, app);
        SSLEngineResult.HandshakeStatus hstatus = result.getHandshakeStatus();
        SSLEngineResult.Status status = result.getStatus();
        
        while(hstatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP ||
              hstatus == SSLEngineResult.HandshakeStatus.NEED_TASK) {

            if(hstatus == SSLEngineResult.HandshakeStatus.NEED_TASK) {
                hstatus = sslTasks();
                continue;
            }

            if(hstatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
                if(status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                    net.compact();
                    assertNotEof(channel.read(net));
                    net.flip();
                }

                result = engine.unwrap(net, app);
                hstatus = result.getHandshakeStatus();
                status = result.getStatus();
            }
        }
            
        return hstatus;
    }

    private SSLEngineResult.HandshakeStatus sslTasks() {
        Runnable task;
        while((task = engine.getDelegatedTask()) != null) {
            task.run();
        }

        return engine.getHandshakeStatus();
    }
}

