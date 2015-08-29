package db.postgresql.protocol.v3.types;

import db.postgresql.protocol.v3.serializers.Udt;
import db.postgresql.protocol.v3.serializers.UdtInput;
import db.postgresql.protocol.v3.serializers.UdtOutput;
import static db.postgresql.protocol.v3.types.UdtHashing.*;

public class LineSegment implements Udt {

    private final Point left;
    private final Point right;

    public Point getLeft() { return left; }
    public Point getRight() { return right; }
    public String getName() { return "lseg"; }
    
    public LineSegment(final UdtInput input) {
        this(input.readUdt(Point.class), input.readUdt(Point.class));
    }

    public LineSegment(final Point left, final Point right) {
        this.left = left;
        this.right = right;
    }

    public void write(final UdtOutput output) {
        output.writeUdt(left);
        output.writeUdt(right);
    }

    @Override
    public String toString() {
        return String.format("%s%s,%s%s", getLeftDelimiter(), left.toString(),
                             right.toString(), getRightDelimiter());
    }
    
    @Override
    public boolean equals(Object o) {
        if(!(o instanceof LineSegment)) {
            return false;
        }

        LineSegment rhs = (LineSegment) o;
        return left.equals(rhs.left) && right.equals(rhs.right);
    }

    @Override
    public int hashCode() {
        return hash(hash(START, left), right);
    }
}
