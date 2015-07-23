package db.postgresql.protocol.v3;

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

    private Notification(final int pid, final String channel, final String payload) {
        super(BackEnd.NotificationResponse);
        this.pid = pid;
        this.channel = channel;
        this.payload = payload;
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public Notification build(final BackEnd backEnd, final int size, final PostgresqlStream stream) {
                return new Notification(stream.getInt(), stream.nullString(), stream.nullString());
            }
        };
}
