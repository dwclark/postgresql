package db.postgresql.protocol.v3.io;

public class NoData extends RuntimeException {

    private static final NoData instance = new NoData();

    private NoData() { }
    
    @Override
    public Throwable fillInStackTrace() {
        return null;
    }

    public static void now() {
        throw instance;
    }
}
