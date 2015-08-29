package db.postgresql.protocol.v3.types;

public class UdtHashing {

    public static final int START = 11317;
    private static final int CONT = 37;
    
    public static int hash(final int soFar, final double val) {
        final long lval = Double.doubleToLongBits(val);
        final int ival = (int) (lval ^ (lval >>> 32));
        return CONT * soFar + ival;
    }

    public static int hash(final int soFar, final Object o) {
        return CONT * soFar + o.hashCode();
    }
}
