package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.PostgresqlStream;

public class Response {

    @FunctionalInterface
    public interface Build {
        Response build(PostgresqlStream stream, int size);
    }

    public BackEnd getBackEnd() {
        return backEnd;
    }

    private final BackEnd backEnd;

    public int getSize() {
        return size;
    }
    
    private final int size;

    protected Response(final BackEnd backEnd, final int size) {
        this.backEnd = backEnd;
        this.size;
    }

    public static final byte NULL = (byte) 0;
}
