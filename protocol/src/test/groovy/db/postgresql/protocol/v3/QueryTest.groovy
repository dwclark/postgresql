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
    
    def "Row Process"() {
        setup:
        Session session = sb.user('noauth').build();
        session.query('select * from items;');
        RowDescription rd = session.next(BackEnd.QUERY)
        Response r;
        while((r = session.next(BackEnd.QUERY)).backEnd != BackEnd.CommandComplete) {
            DataRow.Iterator iter = r.iterator(rd);
            assert(iter.nextInt());
            assert(iter.nextString());
        }
        
        expect:
        rd;
        r;
        r.backEnd == BackEnd.CommandComplete;
        
        cleanup:
        session.close();
    }

    def "Row Array Proccess"() {
        setup:
        Session session = sb.user('noauth').build();
        session.query('select * from items;');
        RowDescription rd = session.next(BackEnd.QUERY)
        Response r;
        while((r = session.next(BackEnd.QUERY)).backEnd != BackEnd.CommandComplete) {
            Object[] ary = r.toArray(rd);
            assert(ary.length == 2);
        }
    }

    def "Row Array All Basic Types"() {
        setup:
        Session session = sb.user('noauth').build();
        session.query('select * from all_types;');
        RowDescription rd = session.next(BackEnd.QUERY)
        Response r;
        while((r = session.next(BackEnd.QUERY)).backEnd != BackEnd.CommandComplete) {
            Object[] ary = r.toArray(rd);
            assert(ary.length == 18);
            println(ary);
        }
    }

}
