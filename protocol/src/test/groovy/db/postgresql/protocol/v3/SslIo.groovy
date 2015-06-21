package db.postgresql.protocol.v3;

import java.net.*;
import javax.net.ssl.*;
import java.nio.channels.*;
import java.nio.*;
import groovy.transform.*;
import java.util.concurrent.*;

@TypeChecked
class SslIo {

    final String host;
    final int port;
    final SSLEngine engine;
    final int appMinBufferSize;
    final int netMinBufferSize;
    final ByteBuffer recvBuffer;
    final ByteBuffer sendBuffer;
    final SocketChannel channel;
    final Selector selector;

    public SslIo(String host, int port, SSLContext context) {
        this.host = host;
        this.port = port;
        
        //Start with the channel in blocking mode, the application is going to have
        //to block initially while the ssl layer starts up.  Might as well
        //get the JVM and the OS to do the blocking for us as needed.
        channel = SocketChannel.open();
        channel.configureBlocking(true);
        channel.connect(new InetSocketAddress(InetAddress.getByName(host), port));

        this.engine = context.createSSLEngine(host, port);
        engine.useClientMode = true;
        engine.wantClientAuth = false;
        this.selector = Selector.open();

        this.appMinBufferSize = engine.session.applicationBufferSize;
        this.netMinBufferSize = engine.session.packetBufferSize;
        this.recvBuffer = netBuffer();
        this.sendBuffer = netBuffer();

        engine.beginHandshake();
        ssl();

        //at this point ssl is going, reset the channel to async and add it to the selector
        channel.configureBlocking(false);
        channel.register(selector, 0);
    }

    public void write(ByteBuffer toWrite) {
        //TODO: call wrap until no bytes remain or status != OK
        assert(toWrite.capacity() >= appMinBufferSize);

        if(sendBuffer.hasRemaining()) {
            io(SelectionKey.OP_WRITE);
        }
        
        SSLEngineResult result = engine.wrap(toWrite, sendBuffer);

        if(result.status == SSLEngineResult.Status.OK) {
            io(SelectionKey.OP_WRITE);
        }
        else if(result.status == SSLEngineResult.Status.BUFFER_OVERFLOW) {
            //do nothing, we will hopefully be able to finish
            //up the operation on the next call to write()
            return;
        }
        else if(result.status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
            throw new BufferUnderflowException();
        }
        else if(result.status == SSLEngineResult.Status.CLOSED) {
            throw new EOFException();
        }

        //TODO: need to handle re-handshake at this point
        
    }

    public void read(ByteBuffer toRead) {
        assert(toRead.capacity() >= appMinBufferSize);

        println("RecvBuffer (before i/o): position(): ${recvBuffer.position()}, " +
                "limit(): ${recvBuffer.limit()} capacity(): ${recvBuffer.capacity()}");
        io(SelectionKey.OP_READ);
        println("RecvBuffer (after i/o): position(): ${recvBuffer.position()}, " +
                "limit(): ${recvBuffer.limit()} capacity(): ${recvBuffer.capacity()}");
        recvBuffer.flip();
        println("RecvBuffer (after flip): position(): ${recvBuffer.position()}, " +
                "limit(): ${recvBuffer.limit()} capacity(): ${recvBuffer.capacity()}");
        SSLEngineResult result;
        while(recvBuffer.hasRemaining() &&
              (result = engine.unwrap(recvBuffer, toRead)).status == SSLEngineResult.Status.OK) {
            println("RecvBuffer (after unwrap): position(): ${recvBuffer.position()}, " +
                    "limit(): ${recvBuffer.limit()} capacity(): ${recvBuffer.capacity()}");
        }
        
        recvBuffer.compact();
        println("RecvBuffer (after compact): position(): ${recvBuffer.position()}, " +
                "limit(): ${recvBuffer.limit()} capacity(): ${recvBuffer.capacity()}");
        toRead.flip();
        if(result.status == SSLEngineResult.Status.OK ||
           result.status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
            println("Read status is: ${result.status}");
        }
        else if(result.status == SSLEngineResult.Status.BUFFER_OVERFLOW) {
            throw new BufferOverflowException();
        }
        else if(result.status == SSLEngineResult.Status.CLOSED) {
            throw new EOFException();
        }
        
        //TODO: Need to handle re-handshake at this point
    }

    public boolean needMore(SSLEngineResult result) {
        if(result.status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
            return true;
        }
        else {
            return false;
        }
    }

    private void io(int ops) {
        println("Registering channel with selector, ops: ${ops}");
        channel.register(selector, ops);
        int count = selector.select(30_000);
        Iterator iter = selector.selectedKeys().iterator();
        if(!iter.hasNext()) {
            println("No channels returned from selector, leaving io");
            return;
        }

        SelectionKey key = iter.next();
        iter.remove();
        if(!key.valid) {
            println("Channel key was invalid, leaving io");
            return;
        }

        if(key.readable) {
            int numBytes = channel.read(recvBuffer);
            println("Received ${numBytes} bytes");
            if(numBytes == -1) {
                throw new EOFException("Reading on closed channel");
            }
        }

        if(key.writable) {
            sendBuffer.flip();
            int numBytes = channel.write(sendBuffer);
            println("Sent ${numBytes} bytes");
            sendBuffer.compact();
        }
    }

    private ByteBuffer netBuffer() {
        return ByteBuffer.allocate(netMinBufferSize);
    }

    private boolean needHandshake(SSLEngineResult.HandshakeStatus status) {
        return !(status == SSLEngineResult.HandshakeStatus.FINISHED ||
                 status == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING);
    }
    
    private void ssl() {
        ByteBuffer appSendBuffer = ByteBuffer.allocate(0);
        ByteBuffer appRecvBuffer = ByteBuffer.allocate(appMinBufferSize);

        println("@@@@@@@@@@@@@@@@@@ handshake #1: ${engine.handshakeStatus}");
        SSLEngineResult.HandshakeStatus hstatus = engine.handshakeStatus;
        while(needHandshake(hstatus)) {
            println("In ssl::while, hstatus: ${hstatus}");
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

    private void assertNotEof(final int num) {
        println("@@@@@@@@@@@@@@@@@@ I/O of ${num} bytes");
        if(num == -1) {
            throw new EOFException();
        }
    }
    
    private SSLEngineResult.HandshakeStatus sslWrap(ByteBuffer app, ByteBuffer net) {
        println("@@@@@@@@@@@@@@@@@@ Starting sslWrap()");
        app.clear(); net.clear();
        SSLEngineResult result = engine.wrap(app, net);
        net.flip();
        while(net.hasRemaining()) {
            assertNotEof(channel.write(net))
        }

        return result.handshakeStatus;
    }

    private SSLEngineResult.HandshakeStatus sslUnwrap(ByteBuffer app, ByteBuffer net) {
        println("@@@@@@@@@@@@@@@@@@ Starting sslUnwrap()");
        app.clear(); net.clear();
        assertNotEof(channel.read(net));
        net.flip();
        SSLEngineResult result = engine.unwrap(net, app);
        SSLEngineResult.HandshakeStatus hstatus = result.handshakeStatus;
        SSLEngineResult.Status status = result.status;
        
        while(hstatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP ||
              hstatus == SSLEngineResult.HandshakeStatus.NEED_TASK) {
            println("@@@@@@@@@@@@@@@@@@ hstatus: ${hstatus}, status: ${status}");

            if(hstatus == SSLEngineResult.HandshakeStatus.NEED_TASK) {
                println("@@@@@@@@@@@@@@@@@@ Running tasks from inside sslUnwrap()");
                hstatus = sslTasks();
                continue;
            }

            if(hstatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
                if(status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                    println("@@@@@@@@@@@@@@@@@@ Buffer underflow inside if sslUnwrap()");
                    net.compact();
                    assertNotEof(channel.read(net));
                    net.flip();
                }

                println("@@@@@@@@@@@@@@@@@@ Doing unwrap (again) inside of sslUnwrap()");
                result = engine.unwrap(net, app);
                hstatus = result.handshakeStatus;
                status = result.status;
            }
        }
            
        return hstatus;
    }

    private SSLEngineResult.HandshakeStatus sslTasks() {
        println("@@@@@@@@@@@@@@@@@@ Starting sslTasks()");
        Runnable task;
        while((task = engine.delegatedTask) != null) {
            println("@@@@@@@@@@@@@@@@@@ Executing task synchronously");
            task.run();
        }

        return engine.handshakeStatus;
    }
}
