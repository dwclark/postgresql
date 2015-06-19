package db.postresql.protocol.v3;

import spock.lang.*;
import java.nio.channels.*;
import java.nio.charset.*;
import javax.net.ssl.*;
import java.net.*;

class SslTest extends Specification {

    SSLEngineManager manager;
    Charset charset = Charset.forName('UTF-8');
    
    def setup() {
        SocketChannel channel = SocketChannel.open();
        channel.connect(new InetSocketAddress(InetAddress.getByName('www.google.com'), 443));
        SSLContext context = SSLContext.getDefault();
        SSLEngine engine = context.createSSLEngine();
        engine.useClientMode = true;
        engine.wantClientAuth = false;
        manager = new SSLEngineManager(channel, engine);
    }

    def "Do Get"() {
        setup:
        def send = 'GET / HTTP/1.1\r\n'.getBytes(charset);
        int sent = 0;
        println("First Remaining: ${manager.appSendBuffer.remaining()}");
        manager.appSendBuffer.put(send);
        while((sent += manager.write()) != send.length) {
            println("Total sent so far: ${sent}")
            println("Remaining: ${manager.appSendBuffer.remaining()}");
        }

        StringBuilder builder = new StringBuilder(32768);
        def atEnd = { -> buider.indexOf('</html>') != -1; };
        while(!atEnd()) {
            println("Attempting to read");
            int read = manager.read();
            if(read > 0) {
                manager.appRecvBuffer.flip();
                byte[] bytes = new byte[manager.appRecvBuffer.remaining()];
                manager.appRecvBuffer.get(bytes);
                builder.append(new String(bytes, charset));
                manager.appRecvBuffer.compact();
            }
        }

        println("Received: ")
        println(builder.toString());
    }
}
