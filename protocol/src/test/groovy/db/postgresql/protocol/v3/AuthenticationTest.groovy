package db.postgresql.protocol.v3;

import spock.lang.*;
import java.nio.channels.*;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.security.*;
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
        Session session = sb.user('noauth').build().connect().authenticate();

        expect:
        session.parameterStatuses;
        
        cleanup:
        session.close();
    }

    def "MD5 Payload"() {
        setup:
        String user = 'david';
        String password = 'qwerty12345';
        Charset c = Charset.forName('UTF-8');
        byte[] salt = 'aK*r'.getBytes(c);
        String payload = AuthenticationMessage.Md5.payload(c, user, password, salt);

        expect:
        payload == 'md53021e9c7078798c92184ad247ec9e6b6';
    }

    def "Clear Text Password"() {
        setup:
        def user = 'clearauth';
        def password = 'clearauth';
        Session session = sb.user(user).password(password).build().connect().authenticate();

        expect:
        session.parameterStatuses;

        cleanup:
        session.close();
    }

    def "MD5 Password"() {
        setup:
        def user = 'md5auth';
        def password = 'md5auth';
        Session session = sb.user(user).password(password).build().connect().authenticate();
        println(session.parameterStatuses);
        
        expect:
        session.parameterStatuses;

        cleanup:
        session.close();
    }
}
