package db.postgresql.protocol.v3

import db.postgresql.protocol.v3.serializers.Serializer;
import spock.lang.*
import db.postgresql.protocol.v3.types.*;

class QueryTest extends Specification {

    @Shared Session session;

    def setupSpec() {
        session = Helper.noAuth();
    }

    def cleanupSpec() {
        session.close();
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
        List results = session.simple('').manyResults(QueryTest.&toArray);

        expect:
        !results;
    }

    def "Row Process"() {
        setup:
        List results = session.simple('select * from items;').manyResults(QueryTest.&toArray);

        expect:
        results;
        results.size() == 2;
    }

    def "Multi Row Process"() {
        setup:
        List results = session.simple('select * from items; ;select * from items;').manyResults(QueryTest.&toArray);

        expect:
        results;
        results.size() == 4;
    }

    def "Row Array Proccess"() {
        setup:
        List list = session.simple('select * from items;').manyRows { DataRow dataRow -> dataRow.toArray(); };

        expect:
        list;
        list.size() == 2;
        list[0] instanceof Object[];
    }

    def "Row Array All Basic Types"() {
        setup:
        List list = session.simple('select * from all_types;').manyRows { DataRow dataRow -> dataRow.toArray(); };

        expect:
        list;
        list.every { ary -> ary.length == 18; };
    }

    def "Geometry Query"() {
        setup:
        List list = session.simple('select * from geometry_types;').singleRow { DataRow dataRow -> dataRow.toArray(); };
        
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
        List list = session.simple('select int_array, string_array from my_arrays').singleRow {
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

    def "Udt Map Query For Items"() {
        setup:
        List<UdtMap> list = session.simple('select items from items').manyRows { DataRow row -> row.iterator().next(); };
        
        expect:
        list;
        list.size() == 2;
        list.every { it instanceof UdtMap; }
    }

    def "Udt Map Query For Geometry Types"() {
        setup:
        List<UdtMap> list = session.simple('select geometry_types from geometry_types').manyRows {
            DataRow row -> row.iterator().next(); };
        
        expect:
        list.size() == 1;
        list.every { it instanceof UdtMap; }
    }

    def "Udt Map Query For Nested Types"() {
        setup:
        List<UdtMap> list = session.simple('select the_person from persons').manyRows {
            DataRow row -> row.iterator().next(); };
        UdtMap entry = list[0];
        
        expect:
        list.size() == 1;
        entry.the_address.lat_long == new Point(45.0d, 45.0d);
    }
}
