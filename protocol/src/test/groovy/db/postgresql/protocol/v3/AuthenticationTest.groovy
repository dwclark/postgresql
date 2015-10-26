package db.postgresql.protocol.v3

import db.postgresql.protocol.v3.io.PostgresqlStream;
import spock.lang.*

import java.nio.ByteBuffer;
import java.nio.charset.Charset

import static db.postgresql.protocol.v3.ssl.ContextCreation.*;

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
        Session session = sb.user('noauth').build();

        expect:
        session.parameterStatuses;
        session.pid;
        session.secretKey;
        
        cleanup:
        session.close();
    }

    def "MD5 Payload"() {
        setup:
        String user = 'david';
        String password = 'qwerty12345';
        Charset c = Charset.forName('UTF-8');
        ByteBuffer salt = ByteBuffer.wrap('aK*r'.getBytes(c));
        String payload = PostgresqlStream.md5Hash(c, user, password, salt);

        expect:
        payload == 'md53021e9c7078798c92184ad247ec9e6b6';
    }

    def "MD5 Payload 35 Chars"() {
        setup:
        String user = 'md5auth';
        String password = user;
        Charset c = Charset.forName('UTF-8');
        ByteBuffer salt = ByteBuffer.wrap([ 117, -75, -60, 31 ] as byte[]);
        String payload = PostgresqlStream.md5Hash(c, user, password, salt);

        expect:
        payload.length() == 35;
    }

    def "Clear Text Password"() {
        setup:
        def user = 'clearauth';
        def password = 'clearauth';
        Session session = sb.user(user).password(password).build();

        expect:
        session.parameterStatuses;
        session.pid;
        session.secretKey;

        cleanup:
        session.close();
    }

    def "MD5 Password"() {
        setup:
        def user = 'md5auth';
        def password = 'md5auth';
        Session session = sb.user(user).password(password).build();
        
        expect:
        session.parameterStatuses;
        session.pid;
        session.secretKey;

        cleanup:
        session?.close();
    }

    def "Duplicate With MD5 Password"() {
        setup:
        def user = 'md5auth';
        def password = 'md5auth';
        Session session = sb.user(user).password(password).build();
        Session session2 = session.duplicate();
        
        expect:
        session.parameterStatuses;
        session.pid;
        session.secretKey;
        session2.parameterStatuses;
        session2.pid;
        session2.secretKey;
        session.pid != session2.pid;
        session.secretKey != session2.secretKey;

        cleanup:
        session?.close();
        session2?.close();
    }

    def "SSL Clear Text Password"() {
        setup:
        def user = 'clearauth';
        def password = 'clearauth';
        Session session = sb.user(user).password(password).sslContext(noCert()).build();

        expect:
        session.parameterStatuses;
        session.pid;
        session.secretKey;

        cleanup:
        session?.close();
    }

    def "SSL MD5 Password"() {
        setup:
        def user = 'md5auth';
        def password = 'md5auth';
        Session session = sb.user(user).password(password).sslContext(noCert()).build();
        
        expect:
        session.parameterStatuses;
        session.pid;
        session.secretKey;
        
        cleanup:
        session?.close();
    }
}
