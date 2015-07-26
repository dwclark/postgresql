package db.postgresql.protocol.v3.serializers;

import spock.lang.*;
import java.nio.*;

class Point implements Udt {

    double x;
    double y;

    String name = 'point';

    public void read(UdtInput input) {
        x = input.readDouble();
        y = input.readDouble();
    }

    public void write(UdtOutput output) {
        throw new UnsupportedOperationException();
    }
}

class PointTest extends Specification {

    def point1 = '(1,2)';
    def point2 = '(1.8,23.6)';
    
    def "Test Point 1"() {
        setup:
        UdtParser parser = new UdtParser(point1);
        Point p = parser.readUdt(Point);

        expect:
        p.x == 1;
        p.y == 2;
    }

    def "Test Point 2"() {
        setup:
        UdtParser parser = new UdtParser(point2);
        Point p = parser.readUdt(Point);

        expect:
        p.x == 1.8d;
        p.y == 23.6d;
    }
}
