package db.postgresql.protocol.v3.serializers;

import spock.lang.*;
import java.nio.*;
import db.postgresql.protocol.v3.types.*;
import db.postgresql.protocol.v3.Session;
import db.postgresql.protocol.v3.Helper;

class GeometryTest extends Specification {

    @Shared Session session;

    def setupSpec() {
        session = Helper.noAuth();
    }

    def cleanupSpec() {
        session.close();
    }

    public UdtParser parser(String str) {
        return UdtParser.forGeometry(session, str);
    }
    
    def "Test Point 1"() {
        setup:
        Point p = parser('(1,2)').read(Point);
        final Point shouldBe = new Point(1,2);
        
        expect:
        p == shouldBe;
        p.hashCode() == shouldBe.hashCode();
        p.toString();
    }

    def "Test Point 2"() {
        setup:
        Point p = parser('(1.8,23.6)').read(Point);
        Point shouldBe = new Point(1.8d, 23.6d);
        
        expect:
        p == shouldBe;
        p.hashCode() == shouldBe.hashCode();
        p.toString();
    }

    def "Test Line 1"() {
        setup:
        Line line = parser('{1,2,3}').read(Line);
        Line shouldBe = new Line(1d, 2d, 3d);
        
        expect:
        line == shouldBe;
        line.hashCode() == shouldBe.hashCode();
        line.toString();
    }

    def "Test Line 2"() {
        setup:
        Line line = parser('{5.5,7.7,10.0}').read(Line);
        Line shouldBe = new Line(5.5d, 7.7d, 10.0d);
        
        expect:
        line == shouldBe;
        line.hashCode() == shouldBe.hashCode();
        line.toString();
    }

    def "Test Line Segment 1"() {
        setup:
        LineSegment lseg = parser('((1,2),(3,4))').read(LineSegment);
        LineSegment shouldBe = new LineSegment(new Point(1d,2d), new Point(3d,4d));
        
        expect:
        lseg == shouldBe;
        lseg.hashCode() == shouldBe.hashCode();
        lseg.toString();
    }

    def "Test Line Segment 2"() {
        setup:
        LineSegment lseg = parser('((1.1,2.2),(3.3,4.4))').read(LineSegment);
        LineSegment shouldBe = new LineSegment(new Point(1.1d,2.2d), new Point(3.3d,4.4d));
        
        expect:
        lseg == shouldBe;
        lseg.hashCode() == shouldBe.hashCode();
        lseg.toString();
    }

    def "Test Box 1"() {
        setup:
        Box box = new BoxSerializer(session).fromString('(1,1),(0,0)')
        Box shouldBe = new Box(new Point(1d,1d), new Point(0d,0d));
            
        expect:
        box == shouldBe;
        box.hashCode() == shouldBe.hashCode();
        box.toString();
    }

    def "Test Box 2"() {
        setup:
        Box box = new BoxSerializer(session).fromString('(1,1),(-1.1,-1.1)');
        Box shouldBe = new Box(new Point(1d,1d), new Point(-1.1d,-1.1d));
        
        expect:
        box == shouldBe;
        box.hashCode() == shouldBe.hashCode();
        box.toString();
    }

    def "Test Open Path 1"() {
        setup:
        Path p = parser('[(0,0),(1,1),(0,2)]').read(Path);
        Path shouldBe = new Path([ new Point(0,0), new Point(1,1), new Point(0,2)], true);
        
        expect:
        p == shouldBe;
        p.hashCode() == shouldBe.hashCode();
        p.toString();
    }

    def "Test Open Path 2"() {
        setup:
        Path p = parser('[(0.1,0.1),(1.1,1.1),(0.1,2.1)]').read(Path);
        Path shouldBe = new Path([ new Point(0.1d,0.1d), new Point(1.1d,1.1d), new Point(0.1d,2.1d)], true);
        
        expect:
        p == shouldBe;
        p.hashCode() == shouldBe.hashCode();
        p.toString();
    }

    def "Test Closed Path 1"() {
        setup:
        Path p = parser('((0,0),(1,1),(0,2))').read(Path);
        Path shouldBe = new Path([ new Point(0,0), new Point(1,1), new Point(0,2)], false);
        
        expect:
        p == shouldBe;
        p.hashCode() == shouldBe.hashCode();
        p.toString();
    }

    def "Test Closed Path 2"() {
        setup:
        Path p = parser('((0.1,0.1),(1.1,1.1),(0.1,2.1))').read(Path);
        Path shouldBe = new Path([ new Point(0.1d,0.1d), new Point(1.1d,1.1d), new Point(0.1d,2.1d)], false);
        
        expect:
        p == shouldBe;
        p.hashCode() == shouldBe.hashCode();
        p.toString();
    }

    def "Test Polygon 1"() {
        setup:
        Polygon p = parser('((0,0),(1,1),(0,2))').read(Polygon);
        Polygon shouldBe = new Polygon([ new Point(0,0), new Point(1,1), new Point(0,2)]);
        
        expect:
        p == shouldBe;
        p.hashCode() == shouldBe.hashCode();
        p.toString();
    }

    def "Test Polygon 2"() {
        setup:
        Polygon p = parser('((0.1,0.1),(1.1,1.1),(0.1,2.1))').read(Polygon);
        Polygon shouldBe = new Polygon([ new Point(0.1d,0.1d), new Point(1.1d,1.1d), new Point(0.1d,2.1d)]);
        
        expect:
        p == shouldBe;
        p.hashCode() == shouldBe.hashCode();
        p.toString();
    }

    def "Test Circle 1"() {
        setup:
        Circle c = parser('<(1,1),5>').read(Circle);
        Circle shouldBe = new Circle(new Point(1d,1d), 5d);
        
        expect:
        c == shouldBe;
        c.hashCode() == shouldBe.hashCode();
        c.toString();
    }
}
