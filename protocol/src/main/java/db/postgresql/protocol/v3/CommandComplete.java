package db.postgresql.protocol.v3;

import java.nio.ByteBuffer;

public class CommandComplete extends Response {

    enum Action { INSERT, DELETE, UPDATE, SELECT, MOVE, FETCH, COPY; }

    private final Action action;
    private final int rows;
    private final int oid;
    
    public int getOid() {
        return oid;
    }

    public int getRows() {
        return rows;
    }

    public Action getAction() {
        return action;
    }
    
    private CommandComplete(final PostgresqlStream stream) {
        super(BackEnd.CommandComplete);
        final String[] ary = stream.nullString().split(" ");
        this.action = Action.valueOf(ary[0]);
        this.rows = Integer.valueOf(ary[1]);
        this.oid = (ary.length == 3) ? Integer.valueOf(ary[2]) : 0;
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public CommandComplete build(final BackEnd backEnd, final int size, final PostgresqlStream stream) {
                return new CommandComplete(stream);
            }
        };
}
