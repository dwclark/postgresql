package db.postgresql.protocol.v3;

import java.nio.ByteBuffer;
import db.postgresql.protocol.v3.io.PostgresqlStream;

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
    
    public CommandComplete(final PostgresqlStream stream, final int size) {
        super(BackEnd.CommandComplete, size);
        final String[] ary = stream.nullString().split(" ");
        if(ary.length == 2) {
            action = Action.valueOf(ary[0]);
            rows = 0;
            oid = Integer.parseInt(ary[1]);
        }
        else {
            action = Action.valueOf(ary[0]);
            rows = Integer.parseInt(ary[1]);
            oid = Integer.parseInt(ary[2]);
        }
    }
}
