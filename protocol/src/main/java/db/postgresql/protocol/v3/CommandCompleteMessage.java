package db.postgresql.protocol.v3;

import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import static db.postgresql.protocol.v3.BackEndFormatter.fromNullString;

public class CommandCompleteMessage extends BackEndMessage {

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
    
    public CommandCompleteMessage(final BackEnd backEnd, final Action action, final int oid, final int rows) {
        super(backEnd);
        this.action = action;
        this.oid = oid;
        this.rows = rows;
    }

    public CommandCompleteMessage(final BackEnd backEnd, final Action action, final int rows) {
        this(backEnd, action, 0, rows);
    }

    public static final BackEndBuilder builder = new BackEndBuilder() {
            public CommandCompleteMessage read(BackEnd backEnd, int size, ScatteringByteChannel channel) {
                ByteBuffer buffer = BackEndFormatter.read(size, channel);
                String[] ary = fromNullString(buffer, size).split(" ");
                if(ary.length == 2) {
                    return new CommandCompleteMessage(backEnd, Action.valueOf(ary[0]), Integer.valueOf(ary[1]));
                }
                else {
                    return new CommandCompleteMessage(backEnd, Action.valueOf(ary[0]),
                                                      Integer.valueOf(ary[1]), Integer.valueOf(ary[2]));
                }
            }
        };
}
