package db.postgresql.protocol.v3;

import spock.lang.*;
import java.nio.channels.*;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.security.*;
import static db.postgresql.protocol.v3.ssl.ContextCreation.*;

class PrepareAndBindTest extends Specification {

    def host = '127.0.0.1';
    def port = 5432;
    def database = 'testdb';

    Session.Builder sb;

    def setup() {
        sb = new Session.Builder().host(host).port(port).database(database);
    }

    def "Test Parse"() {
        setup:
        Session session = sb.user('noauth').foreground();
        PostgresqlStream stream = session.stream;
        
        int[] oids = [ 0, 0 ] as int[];
        stream.parse('_1', 'select * from items;', [] as int[]);
        stream.sync();
        Response r = stream.next(BackEnd.QUERY);
        println(r.backEnd);

        expect:
        r;
        //r.backEnd == BackEnd.ReadyForQuery;
        //pr;
    }
}

