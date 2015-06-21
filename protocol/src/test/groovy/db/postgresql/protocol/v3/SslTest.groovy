package db.postresql.protocol.v3;

import spock.lang.*;
import java.nio.channels.*;
import java.nio.charset.*;
import javax.net.ssl.*;
import java.net.*;
import java.nio.*;

class SslTest extends Specification {

    SslIo io;
    Charset charset = Charset.forName('UTF-8');
    
    def setup() {
        //io = new SslIo('www.google.com', 443, SSLContext.getDefault());
    }

    def "Do Init"() {
        setup:
        SslIo io = new SslIo('www.google.com', 443, SSLContext.getDefault());

        expect:
        io;
    }

    /*def "Do Get"() {
        setup:
        def send = 'GET / HTTP/1.1\r\n'.getBytes(charset);
        ByteBuffer sendBuffer = ByteBuffer.allocate(io.appMinBufferSize);
        ByteBuffer recvBuffer = ByteBuffer.allocate(io.appMinBufferSize);
        sendBuffer.put(send);
        while(sendBuffer.hasRemaining()) {
            io.write(sendBuffer);
        }

        StringBuilder builder = new StringBuilder(32768);
        while(builder.indexOf('</html>') == -1) {
            io.read(recvBuffer);
            recvBuffer.flip();
            byte[] bytes = new byte[recvBuffer.remaining()];
            recvBuffer.get(bytes);
            builder.append(new String(bytes, charset));
            recvBuffer.compact();
        }
        
        println("Received: ")
        println(builder.toString());
        }*/
}
