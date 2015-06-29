package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.util.Map;

public class Notification extends Response {

    public int getPid() {
        return buffer.getInt(0);
    }

    public String getChannel() {
        final int next = nextNull(4);
        return nullString(4, next);
    }

    public String getPayload() {
        final int startAt = nextNull(4) + 1;
        final int next = nextNull(startAt);
        return nullString(startAt, next);
    }

    private Notification() {
        super(BackEnd.NotificationResponse);
    }

    private Notification(Notification toCopy) {
        super(toCopy);
    }

    @Override
    public Notification copy() {
        return new Notification(this);
    }

    private static final ThreadLocal<Notification> tlData = new ThreadLocal<Notification>() {
            @Override protected Notification initialValue() {
                return new Notification();
            }
        };

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public Notification build(final BackEnd backEnd, final int size, final Stream stream) {
                return (Notification) tlData.get().reset(stream.getRecord(size), stream.getEncoding());
            }
        };
}
