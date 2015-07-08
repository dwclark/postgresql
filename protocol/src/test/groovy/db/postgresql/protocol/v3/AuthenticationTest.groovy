package db.postgresql.protocol.v3;

import spock.lang.*;
import java.nio.channels.*;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.*;
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

    @Ignore
    def "SSL Clear Text Password"() {
        setup:
        def user = 'clearauth';
        def password = 'clearauth';
        Session session = sb.user(user).password(password).sslContext(noCert()).build();
        session.foreground().startup(session.initKeysValues);
        while(session.next(EnumSet.noneOf(BackEnd)).backEnd != BackEnd.ReadyForQuery) { }

        expect:
        session.parameterStatuses;
        session.pid;
        session.secretKey;

        cleanup:
        session?.close();
    }

    @Ignore
    def "SSL MD5 Password"() {
        setup:
        def user = 'md5auth';
        def password = 'md5auth';
        Session session = sb.user(user).password(password).sslContext(noCert()).build();
        session.foreground().startup(session.initKeysValues);
        while(session.next(EnumSet.noneOf(BackEnd)).backEnd != BackEnd.ReadyForQuery) { }
        
        expect:
        session.parameterStatuses;
        session.pid;
        session.secretKey;
        session.lastStatus == TransactionStatus.IDLE;
        
        cleanup:
        session?.close();

    }
}
