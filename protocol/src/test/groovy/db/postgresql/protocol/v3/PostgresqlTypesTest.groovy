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

        when:
        ExtendedQuery insert = new ExtendedQuery('insert into extended_types (my_bits, my_uuid) ' +
                                                 'values ($1, $2)', session);
        BitSet bitSet = new BitSet(3);
        bitSet.set(0, true);
        bitSet.set(1, false);
        bitSet.set(2, true);
        UUID uuid = UUID.randomUUID();
        insert.execute([ [ insert.bind(bitSet), insert.bind(uuid) ] as Bindable[] ]);
        insert.noResults();

        then:
        insert.complete.rows == 1;
        insert.complete.action == CommandComplete.Action.INSERT;

        when:
        ExtendedQuery del = new ExtendedQuery('delete from extended_types where my_bits = $1', session);
        del.execute([ [ del.bind(bitSet) ] as Bindable[] ]).noResults();

        then:
        del.complete.rows == 1;
        del.complete.action == CommandComplete.Action.DELETE;
    }
}

