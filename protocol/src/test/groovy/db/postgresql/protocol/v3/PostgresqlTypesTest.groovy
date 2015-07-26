package db.postgresql.protocol.v3;

import spock.lang.*;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.security.*;
import static db.postgresql.protocol.v3.ssl.ContextCreation.*;
import db.postgresql.protocol.v3.serializers.*;

class PostgresqlTypesTest extends Specification {

    def host = '127.0.0.1';
    def port = 5432;
    def database = 'testdb';
    def testBitSet = new BitSet(6);

    Session.Builder sb;

    def setup() {
        sb = new Session.Builder().host(host).port(port).database(database);
        def index = 0;
        testBitSet.set(index++, true);
        testBitSet.set(index++, true);
        testBitSet.set(index++, false);
        testBitSet.set(index++, false);
        testBitSet.set(index++, true);
        testBitSet.set(index++, true);
    }

    def "Test UUID and BitSet"() {
        setup:
        Session session = sb.user('noauth').build();

        when:
        ExtendedQuery select = new ExtendedQuery('select * from extended_types;', session);
        select.execute(Bindable.EMPTY);
        def row = select.singleResult { DataRow.Iterator iter ->
            return [ iter.nextBitSet(), iter.nextUUID() ]; };

        then:
        row;
        row.size() == 2;
        row[0] instanceof BitSet;
        row[0] == testBitSet;
        row[1] instanceof UUID;
        row[1] == UUID.fromString('aa81b166-c60f-4e4e-addb-17414a652733');
        //eq.execute([ [ eq.bind(3), eq.bind('three') ] as Bindable[] ]);
        //eq.noResults();
    }
}

