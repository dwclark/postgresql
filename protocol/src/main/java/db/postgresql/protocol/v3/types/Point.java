package db.postgresql.protocol.v3.types;

import db.postgresql.protocol.v3.serializers.Udt;
import db.postgresql.protocol.v3.serializers.UdtInput;
import db.postgresql.protocol.v3.serializers.UdtOutput;
import static db.postgresql.protocol.v3.types.UdtHashing.*;

public class Point implements Udt {

    private final double x;
    private final double y;

    public double getX() { return x; }
    public double getY() { return y; }
    
    public String getName() { return "point"; }

    public Point(final UdtInput input) {
        this(input.readDouble(), input.readDouble());
    }

    public Point(final double x, final double y) {
        this.x = x;
        this.y = y;
    }

    public void write(final UdtOutput output) {
        output.writeDouble(x);
        output.writeDouble(y);
    }

    @Override
    public String toString() {
        return String.format("%s%d,%d%s", getLeftDelimiter(), x, y, getRightDelimiter());
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof Point)) {
            return false;
        }

        Point rhs = (Point) o;
        return ((x == rhs.x) && (y == rhs.y));
    }

    @Override
    public int hashCode() {
        return hash(hash(START, x), y);
    }
}
