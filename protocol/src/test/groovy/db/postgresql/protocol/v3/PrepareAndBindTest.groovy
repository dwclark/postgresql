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
        Session session = sb.user('noauth').build();
        
        int[] oids = [ 0, 0 ] as int[];
        session.parse('_1', 'select * from items;', [] as int[]);
        session.sync();
        Response r = session.next(BackEnd.QUERY);
        println(r.backEnd);

        expect:
        r;
    }
}

