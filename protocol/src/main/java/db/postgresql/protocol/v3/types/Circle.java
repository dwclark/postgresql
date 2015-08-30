package db.postgresql.protocol.v3.types;

import db.postgresql.protocol.v3.serializers.Udt;
import db.postgresql.protocol.v3.serializers.UdtInput;
import db.postgresql.protocol.v3.serializers.UdtOutput;
import static db.postgresql.protocol.v3.types.UdtHashing.*;

public class Circle implements Udt {

    @Override
    public char getLeftDelimiter() { return '<'; }

    @Override
    public char getRightDelimiter() { return '>'; }
    
    private final Point center;
    private final double radius;

    public Point getCenter() { return center; }
    public double getRadius() { return radius; }
    public String getName() { return "circle"; }
    
    public Circle(final UdtInput input) {
        this(input.readUdt(Point.class), input.readDouble());
    }

    public Circle(final Point center, final double radius) {
        this.center = center;
        this.radius = radius;
    }

    public void write(final UdtOutput output) {
        output.writeUdt(center);
        output.writeDouble(radius);
    }

    @Override
    public String toString() {
        return String.format("%s%d,%d%s", getLeftDelimiter(), center.toString(), radius, getRightDelimiter());
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof Circle)) {
            return false;
        }

        Circle rhs = (Circle) o;
        return (center.equals(rhs.center) && radius == rhs.radius);
    }

    @Override
    public int hashCode() {
        return hash(hash(START, center), radius);
    }
}
