package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;

public class CommandComplete extends Response {

    enum Action { INSERT, DELETE, UPDATE, SELECT, MOVE, FETCH, COPY; }

    private final int oid;
    private final int rows;
    private final Action action;
    
    public int getOid() {
        return oid;
    }

    public int getRows() {
        return rows;
    }

    public Action getAction() {
        return action;
    }
    
    public CommandComplete(final BackEnd backEnd, final Action action, final int oid, final int rows) {
        super(backEnd);
        this.action = action;
        this.oid = oid;
        this.rows = rows;
    }

    public CommandComplete(final BackEnd backEnd, final Action action, final int rows) {
        this(backEnd, action, 0, rows);
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public CommandComplete build(BackEnd backEnd, int size, Stream stream) {
                String[] ary = stream.nullString(size).split(" ");
                if(ary.length == 2) {
                    return new CommandComplete(backEnd, Action.valueOf(ary[0]), Integer.valueOf(ary[1]));
                }
                else {
                    return new CommandComplete(backEnd, Action.valueOf(ary[0]),
                                               Integer.valueOf(ary[1]), Integer.valueOf(ary[2]));
                }
            }
        };
}
