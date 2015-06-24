package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.util.Map;

public class Notification extends Response {

    private final int pid;
    private final String channel;
    private final String payload;

    public int getPid() {
        return pid;
    }

    public String getChannel() {
        return channel;
    }

    public String getPayload() {
        return payload;
    }

    public Notification(final BackEnd backEnd, final int pid, final String channel, final String payload) {
        super(backEnd);
        this.pid = pid;
        this.channel = channel;
        this.payload = payload;
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public Notification build(final BackEnd backEnd, final int size, final Stream stream) {
                final int pid = stream.getInt();
                final byte[] bytes = stream.get(new byte[size]);
                Map.Entry<String,String> pair = PostgresqlStream.nullPair(bytes, stream.getEncoding());
                return new Notification(backEnd, pid, pair.getKey(), pair.getValue());
            }
        };
}
