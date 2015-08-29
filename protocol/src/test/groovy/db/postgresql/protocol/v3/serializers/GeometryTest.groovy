package db.postgresql.protocol.v3.serializers;

import spock.lang.*;
import java.nio.*;
import db.postgresql.protocol.v3.types.*;

class GeometryTest extends Specification {

    def point1 = '(1,2)';
    def point2 = '(1.8,23.6)';

    def line1 = '{1,2,3}';
    def line2 = '{5.5,7.7,10.0}';

    def lseg1 = '((1,2),(3,4))';
    def lseg2 = '((1.1,2.2),(3.3,4.4))';

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

    def "Test Line 1"() {
        setup:
        UdtParser parser = new UdtParser(line1);
        Line line = parser.readUdt(Line);

        expect:
        line.a == 1;
        line.b == 2;
        line.c == 3;
    }

    def "Test Line 2"() {
        setup:
        UdtParser parser = new UdtParser(line2);
        Line line = parser.readUdt(Line);

        expect:
        line.a == 5.5d;
        line.b == 7.7d;
        line.c == 10.0d;
    }

    def "Test Line Segment 1"() {
        setup:
        UdtParser parser = new UdtParser(lseg1);
        LineSegment lseg = parser.readUdt(LineSegment);

        expect:
        lseg.left == new Point(1d,2d);
        lseg.right == new Point(3d,4d);
    }
}
