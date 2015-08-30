package db.postgresql.protocol.v3.types;

import java.util.Iterator;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import db.postgresql.protocol.v3.serializers.Udt;
import db.postgresql.protocol.v3.serializers.UdtInput;
import db.postgresql.protocol.v3.serializers.UdtOutput;
import static db.postgresql.protocol.v3.types.UdtHashing.*;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class Path implements Udt {

    public static final PgType PGTYPE =
        new PgType.Builder().name("path").oid(602).arrayId(1019).build();
    
    private static final char[] OPEN_DELIMITERS = { '[', ']' };
    private static final char[] CLOSED_DELIMITERS = { '(', ')' };

    public char getLeftDelimiter() {
        return open ? OPEN_DELIMITERS[0] : CLOSED_DELIMITERS[1];
    }

    public char getRightDelimiter() {
        return open ? OPEN_DELIMITERS[1] : CLOSED_DELIMITERS[1];
    }

    public String getName() { return PGTYPE.getName(); }
    
    private final List<Point> points;
    private final boolean open;
    
    public List<Point> getPoints() {
        return points;
    }

    public boolean isOpen() { return open; }
    public boolean isClosed() { return !open; }

    public Path(final UdtInput input) {
        this.open = input.getCurrentDelimiter() == OPEN_DELIMITERS[0];
        List<Point> tmp = new ArrayList<>();
        while(input.hasNext()) {
            tmp.add(input.readUdt(Point.class));
        }

        this.points = Collections.unmodifiableList(tmp);
    }

    public Path(final List<Point> points, final boolean open) {
        this.open = open;
        this.points = Collections.unmodifiableList(new ArrayList<>(points));
    }

    public void write(final UdtOutput output) {
        for(Point p : points) {
            output.writeUdt(p);
        }
    }

    @Override
    public String toString() {
        return getLeftDelimiter() +
            points.stream().map(p -> p.toString()).collect(Collectors.joining(",")) +
            getRightDelimiter();
    }

    @Override
    public int hashCode() {
        int result = START;
        for(Point p : points) {
            result = hash(result, p);
        }

        return result;
    }

    protected boolean pointsEqual(final List<Point> otherPoints) {
        if(points.size() != otherPoints.size()) {
            return false;
        }
        
        Iterator<Point> thisIter = points.iterator();
        Iterator<Point> otherIter = otherPoints.iterator();
        while(thisIter.hasNext()) {
            if(!thisIter.next().equals(otherIter.next())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean equals(Object o) {
        if(o == null) {
            return false;
        }
        
        if(!o.getClass().equals(Path.class)) {
            return false;
        }
        
        Path rhs = (Path) o;
        return ((open == rhs.open) && pointsEqual(rhs.points));
    }
}
