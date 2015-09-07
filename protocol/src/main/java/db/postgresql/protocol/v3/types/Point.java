package db.postgresql.protocol.v3.types;

import db.postgresql.protocol.v3.serializers.UdtInput;
import db.postgresql.protocol.v3.serializers.UdtOutput;
import static db.postgresql.protocol.v3.types.UdtHashing.*;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class Point implements Udt {

    public static final PgType PGTYPE =
        new PgType.Builder().name("point").oid(600).arrayId(1017).build();

    private final double x;
    private final double y;

    public double getX() { return x; }
    public double getY() { return y; }
    
    public String getName() { return PGTYPE.getName(); }

    public Point(final UdtInput input) {
        this(input.read(Double.class), input.read(Double.class));
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
        return String.format("%c%f,%f%c", getLeftDelimiter(), x, y, getRightDelimiter());
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
