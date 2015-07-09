package db.postgresql.protocol.v3;

import spock.lang.*;
import java.nio.channels.*;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.security.*;
import static db.postgresql.protocol.v3.ssl.ContextCreation.*;
import db.postgresql.protocol.v3.serializers.*;

class PrepareAndBindTest extends Specification {

    def host = '127.0.0.1';
    def port = 5432;
    def database = 'testdb';

    Session.Builder sb;

    def setup() {
        sb = new Session.Builder().host(host).port(port).database(database);
    }

    @Ignore
    def "Test Parse"() {
        setup:
        Session session = sb.user('noauth').build();
        session.parse('_1', 'select * from items;', [] as int[]);
        session.sync();
        Response r = session.next(BackEnd.QUERY);
        println(r.backEnd);

        expect:
        r;

        cleanup:
        session.close();
    }

    def "Test Full Bind Cycle"() {
        setup:
        Session session = sb.user('noauth').build();
        String insertName = 'insertIt';
        String insertPortal = 'insertPortal';
        String deleteName = 'deleteIt';
        String deletePortal = 'deletePortal';
        
        session.parse(insertName, 'insert into items values ($1, $2)', Session.EMPTY_OIDS);
        session.sync();
        Response r = session.next(BackEnd.QUERY);
        println(r.backEnd); //parse complete
        r = session.next(BackEnd.QUERY);
        println(r.backEnd); //ready for query
        
        IntSerializer intSerializer = session.serializer(IntSerializer);
        StringSerializer strSerializer = session.serializer(StringSerializer);
        Bindable[] inputs = [ intSerializer.bindable(3, Format.TEXT), strSerializer.bindable("three", Format.TEXT) ] as Bindable[];
        session.bind(insertPortal, insertName, inputs, Session.EMPTY_FORMATS).execute(insertPortal);
        session.sync();
        r = session.next(BackEnd.QUERY);
        println(r.backEnd); //bind complete
        r = session.next(BackEnd.QUERY);
        println(r.backEnd); //command completed
        r = session.next(BackEnd.QUERY);
        println(r.backEnd); //ready for query
        
        cleanup:
        session.close();
    }
}

