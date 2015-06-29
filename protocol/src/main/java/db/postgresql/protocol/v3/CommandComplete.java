package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.nio.ByteBuffer;

public class CommandComplete extends Response {

    enum Action { INSERT, DELETE, UPDATE, SELECT, MOVE, FETCH, COPY; }

    private String[] split() {
        return nullString(0, nextNull(0)).split(" ");
    }
    
    public int getOid() {
        String[] ary = split();
        if(ary.length == 2) {
            return 0;
        }
        else {
            return Integer.valueOf(ary[2]);
        }
    }

    public int getRows() {
        String[] ary = split();
        return Integer.valueOf(ary[1]);
    }

    public Action getAction() {
        String[] ary = split();
        return Action.valueOf(ary[0]);
    }
    
    private CommandComplete() {
        super(BackEnd.CommandComplete);
    }

    private CommandComplete(final CommandComplete toCopy) {
        super(toCopy);
    }

    @Override
    public CommandComplete copy() {
        return new CommandComplete(this);
    }
    
    public static final ResponseBuilder builder = new ResponseBuilder() {
            public CommandComplete build(BackEnd backEnd, int size, Stream stream) {
                return (CommandComplete) tlData.get().reset(stream.getRecord(size), stream.getEncoding());
            }
        };

    private static final ThreadLocal<CommandComplete> tlData = new ThreadLocal<CommandComplete>() {
            @Override protected CommandComplete initialValue() {
                return new CommandComplete();
            }
        };
}
