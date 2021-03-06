package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.SslIO;
import spock.lang.*;
import java.nio.channels.*;
import java.nio.charset.*;
import javax.net.ssl.*;
import java.net.*;
import java.nio.*;

class SslTest extends Specification {

    SslIO io;
    Charset charset = Charset.forName('UTF-8');
    
    def "Do Google Test"() {
        setup:
        io = new SslIO('www.google.com', 443, SSLContext.getDefault(), SslIO.noPre);
        StringBuilder builder = new StringBuilder(8192);
        ByteBuffer sendBuffer = ByteBuffer.allocate(io.appMinBufferSize);
        ByteBuffer recvBuffer = ByteBuffer.allocate(io.appMinBufferSize);
        sendBuffer.put('GET / HTTP/1.1\r\nContent-Length: 0\r\n\r\n'.getBytes(charset));
        sendBuffer.flip();
        while(sendBuffer.hasRemaining()) {
            println("Sending sendBuffer in SslTest");
            io.write(sendBuffer);
        }

        sendBuffer.clear();

        while(builder.indexOf('</html>') == -1) {
            println("Reading recvBuffer in SslTest");
            io.read(recvBuffer);
            recvBuffer.flip();
            println("About to read ${recvBuffer.remaining()} bytes");
            byte[] bytes = new byte[recvBuffer.remaining()];
            recvBuffer.get(bytes);
            recvBuffer.clear();
            String recv = new String(bytes, charset);
            builder.append(recv);
            println("Received ${recv}");
        }

        expect:
        builder.indexOf('</html>') != -1;
        println(builder.toString());
    }

    def "Do Init Websites"() {
        when:
        io = new SslIO('www.google.com', 443, SSLContext.getDefault(), SslIO.noPre);
        then:
        io;

        when:
        io = new SslIO('www.yahoo.com', 443, SSLContext.getDefault(), SslIO.noPre);
        then:
        io;

        when:
        io = new SslIO('www.amazon.com', 443, SSLContext.getDefault(), SslIO.noPre);
        then:
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
