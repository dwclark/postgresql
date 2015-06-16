package db.postgresql.protocol.v3;

import spock.lang.*;
import java.nio.channels.*;
import java.net.InetSocketAddress;
import java.net.InetAddress;

class AuthenticationTest extends Specification {

    def host = '127.0.0.1';
    def port = 5432;
    def database = 'testdb';

    Session.Builder sb;

    def setup() {
        sb = new Session.Builder().host(host).port(port).database(database);
    }
    
    def "No Authentication"() {
        setup:
        Session session = sb.user('noauth').build().connect();
        println("Made it here!");
        
        cleanup:
        session.close();
    }
}
