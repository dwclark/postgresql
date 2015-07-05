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

    private Notification(final Stream stream) {
        super(BackEnd.NotificationResponse);
        this.pid = stream.getInt();
        this.channel = stream.nullString();
        this.payload = stream.nullString();
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public Notification build(final BackEnd backEnd, final int size, final Stream stream) {
                return new Notification(stream);
            }
        };
}
