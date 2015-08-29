package db.postgresql.protocol.v3.types;

import db.postgresql.protocol.v3.serializers.Udt;
import db.postgresql.protocol.v3.serializers.UdtInput;
import db.postgresql.protocol.v3.serializers.UdtOutput;
import static db.postgresql.protocol.v3.types.UdtHashing.*;

public class Line implements Udt {

    @Override
    public char getLeftDelimiter() { return '{'; }

    @Override
    public char getRightDelimiter() { return '}'; }
    
    private final double a;
    private final double b;
    private final double c;

    public double getA() { return a; }
    public double getB() { return b; }
    public double getC() { return c; }

    public String getName() { return "line"; }
    
    public Line(final UdtInput input) {
        this(input.readDouble(), input.readDouble(), input.readDouble());
    }

    public Line(final double a, final double b, final double c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public void write(final UdtOutput output) {
        output.writeDouble(c);
        output.writeDouble(b);
        output.writeDouble(c);
    }

    @Override
    public String toString() {
        return String.format("%s%d,%d,%d,%s", getLeftDelimiter(), a, b, c, getRightDelimiter());
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof Line)) {
            return false;
        }

        Line rhs = (Line) o;
        return ((a == rhs.a) && (b == rhs.b) && (c == rhs.c));
    }

    @Override
    public int hashCode() {
        return hash(hash(hash(START, a), b), c);
    }
}
