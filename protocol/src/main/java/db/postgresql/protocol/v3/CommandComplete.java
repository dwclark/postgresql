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
    
    private CommandComplete(final Action action, final int oid, final int rows) {
        super(BackEnd.CommandComplete);
        this.action = action;
        this.oid = oid;
        this.rows = rows;
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public CommandComplete build(final BackEnd backEnd, final int size, final Session session) {
                final String[] ary = session.nullString().split(" ");
                if(ary.length == 2) {
                    return new CommandComplete(Action.valueOf(ary[0]), 0, Integer.valueOf(ary[1]));
                }
                else if(ary.length == 3) {
                    return new CommandComplete(Action.valueOf(ary[0]), Integer.valueOf(ary[1]), Integer.valueOf(ary[2]));
                }
                else {
                    throw new ProtocolException();
                }
            }
        };
}
