package db.postgresql.protocol.v3

import db.postgresql.protocol.v3.serializers.Serializer;
import spock.lang.*
import db.postgresql.protocol.v3.types.*;

class QueryTest extends Specification {

    def host = '127.0.0.1';
    def port = 5432;
    def database = 'testdb';

    Session.Builder sb;

    def setup() {
        sb = new Session.Builder().host(host).port(port).database(database);
    }

    static toArray(DataRow.Iterator iter) {
        def list = [];
        while(iter.hasNext()) {
            list << iter.next();
        }

        return list;
    }

    def "Empty Query"() {
        setup:
        Session session = sb.user('noauth').build();
        List results = new SimpleQuery('', session).manyResults(QueryTest.&toArray);

        expect:
        !results;
        
        cleanup:
        session.close();
    }

    def "Row Process"() {
        setup:
        Session session = sb.user('noauth').build();
        List results = new SimpleQuery('select * from items;', session).manyResults(QueryTest.&toArray);

        expect:
        results;
        results.size() == 2;
        
        cleanup:
        session.close();
    }

    def "Multi Row Process"() {
        setup:
        Session session = sb.user('noauth').build();
        List results = new SimpleQuery('select * from items; ;select * from items;', session).manyResults(QueryTest.&toArray);

        expect:
        results;
        results.size() == 4;
        
        cleanup:
        session.close();
    }

    def "Row Array Proccess"() {
        setup:
        Session session = sb.user('noauth').build();
        List list = new SimpleQuery('select * from items;', session).manyRows { DataRow dataRow -> dataRow.toArray(); };

        expect:
        list;
        list.size() == 2;
        list[0] instanceof Object[];

        cleanup:
        session.close();
    }

    def "Row Array All Basic Types"() {
        setup:
        Session session = sb.user('noauth').build();
        List list = new SimpleQuery('select * from all_types;', session).manyRows { DataRow dataRow -> dataRow.toArray(); };

        expect:
        list;
        list.every { ary -> ary.length == 18; };

        cleanup:
        session.close();
    }

    def "Geometry Query"() {
        setup:
        Session session = sb.user('noauth').build();
        List list = new SimpleQuery('select * from geometry_types;', session).singleRow { DataRow dataRow -> dataRow.toArray(); };
        println(list);
        
        expect:
        list;
        list[1] instanceof Point;
        list[2] instanceof Line;
        list[3] instanceof LineSegment;
        list[4] instanceof Box;
        list[5] instanceof Path && list[5].closed;
        list[6] instanceof Path && list[6].open;
        list[7] instanceof Polygon;
        list[8] instanceof Circle;
    }

    def "Arrays Query"() {
        setup:
        Session session = sb.user('noauth').build();
        List list = new SimpleQuery('select int_array, string_array from my_arrays', session).singleRow {
            DataRow dataRow -> dataRow.toArray(); };
        int[][][] intArray = list[0];
        String[][] strArray = list[1];

        expect:
        strArray[1][1] == 'fuzz';
        strArray[0][1] == 'bar';
        intArray[0][0][0] == 1;
        intArray[1][1][2] == 16;
        intArray[2][0][1] == 22;
    }
}
