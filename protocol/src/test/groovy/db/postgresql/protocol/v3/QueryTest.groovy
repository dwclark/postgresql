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
        RowDescription rd = stream.next(BackEnd.QUERY).copy();
        DataRow dr1 = stream.next(BackEnd.QUERY).copy();
        println(dr1.extractInt(rd, 0));
        println(dr1.extractString(rd, 1));
        DataRow dr2 = stream.next(BackEnd.QUERY);
        CommandComplete cc = stream.next(BackEnd.QUERY);
        
        expect:
        rd && dr1 && dr2 && cc;
        
        cleanup:
        session.close();
    }

}
