package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.PostgresqlStream;
import java.util.Queue;
import java.util.function.BiPredicate;
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

    public Notification(final PostgresqlStream stream, final int size) {
        super(BackEnd.NotificationResponse, size);
        this.pid = stream.getInt();
        this.channel = stream.nullString();
        this.payload = stream.nullString();
    }

    public static BiPredicate<PostgresqlStream,Response> addQueue(final Queue<Notification> queue) {
        return (s,r) -> queue.offer((Notification) r);
    }
}
