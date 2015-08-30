package db.postgresql.protocol.v3.serializers;

import spock.lang.*;
import java.nio.*;
import db.postgresql.protocol.v3.types.*;

class GeometryTest extends Specification {
    
    def "Test Point 1"() {
        setup:
        Point p = new UdtParser('(1,2)').readUdt(Point);

        expect:
        p == new Point(1,2);
    }

    def "Test Point 2"() {
        setup:
        Point p = new UdtParser('(1.8,23.6)').readUdt(Point);

        expect:
        p == new Point(1.8d, 23.6d);
    }

    def "Test Line 1"() {
        setup:
        Line line = new UdtParser('{1,2,3}').readUdt(Line);

        expect:
        line == new Line(1d, 2d, 3d);
    }

    def "Test Line 2"() {
        setup:
        Line line = new UdtParser('{5.5,7.7,10.0}').readUdt(Line);

        expect:
        line == new Line(5.5d, 7.7d, 10.0d);
    }

    def "Test Line Segment 1"() {
        setup:
        LineSegment lseg = new UdtParser('((1,2),(3,4))').readUdt(LineSegment);

        expect:
        lseg == new LineSegment(new Point(1d,2d), new Point(3d,4d));
    }

    def "Test Line Segment 2"() {
        setup:
        LineSegment lseg = new UdtParser('((1.1,2.2),(3.3,4.4))').readUdt(LineSegment);

        expect:
        lseg == new LineSegment(new Point(1.1d,2.2d), new Point(3.3d,4.4d));
    }

    def "Test Box 1"() {
        setup:
        Box box = new UdtParser('((1,1),(0,0))').readUdt(Box);
        
        expect:
        box == new Box(new Point(1d,1d), new Point(0d,0d));
    }

    def "Test Box 2"() {
        setup:
        Box box = new UdtParser('((1,1),(-1.1,-1.1))').readUdt(Box);

        expect:
        box == new Box(new Point(1d,1d), new Point(-1.1d,-1.1d));
    }

    def "Test Open Path 1"() {
        setup:
        Path p = new UdtParser('[(0,0),(1,1),(0,2)]').readUdt(Path);

        expect:
        p == new Path([ new Point(0,0), new Point(1,1), new Point(0,2)], true);
    }

    def "Test Open Path 2"() {
        setup:
        Path p = new UdtParser('[(0.1,0.1),(1.1,1.1),(0.1,2.1)]').readUdt(Path);
        
        expect:
        p == new Path([ new Point(0.1d,0.1d), new Point(1.1d,1.1d), new Point(0.1d,2.1d)], true);
    }

    def "Test Closed Path 1"() {
        setup:
        Path p = new UdtParser('((0,0),(1,1),(0,2))').readUdt(Path);

        expect:
        p == new Path([ new Point(0,0), new Point(1,1), new Point(0,2)], false);
    }

    def "Test Closed Path 2"() {
        setup:
        Path p = new UdtParser('((0.1,0.1),(1.1,1.1),(0.1,2.1))').readUdt(Path);
        
        expect:
        p == new Path([ new Point(0.1d,0.1d), new Point(1.1d,1.1d), new Point(0.1d,2.1d)], false);
    }

    def "Test Polygon 1"() {
        setup:
        Polygon p = new UdtParser('((0,0),(1,1),(0,2))').readUdt(Polygon);

        expect:
        p == new Polygon([ new Point(0,0), new Point(1,1), new Point(0,2)]);
    }

    def "Test Polygon 2"() {
        setup:
        Polygon p = new UdtParser('((0.1,0.1),(1.1,1.1),(0.1,2.1))').readUdt(Polygon);
        
        expect:
        p == new Polygon([ new Point(0.1d,0.1d), new Point(1.1d,1.1d), new Point(0.1d,2.1d)]);
    }

    def "Test Circle 1"() {
        setup:
        Circle c = new UdtParser('<(1,1),5>').readUdt(Circle);

        expect:
        c == new Circle(new Point(1d,1d), 5d);
    }
}
