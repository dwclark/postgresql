package db.postgresql.protocol.v3;

import spock.lang.*;
import java.nio.channels.*;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.security.*;
import static db.postgresql.protocol.v3.ssl.ContextCreation.*;

class QueryTest extends Specification {

    def host = '127.0.0.1';
    def port = 5432;
    def database = 'testdb';

    Session.Builder sb;

    def setup() {
        sb = new Session.Builder().host(host).port(port).database(database);
    }

    
    def "Row Description"() {
        setup:
        Session session = sb.user('noauth').foreground();
        PostgresqlStream stream = session.stream;
        stream.query('select * from items;');
        RowDescription rd = stream.next(BackEnd.QUERY);

        expect:
        rd;
        
        cleanup:
        session.close();
    }

}
