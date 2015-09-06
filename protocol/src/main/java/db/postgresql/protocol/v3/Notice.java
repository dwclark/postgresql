package db.postgresql.protocol.v3;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class Notice extends Response {

    final private Map<NoticeType,String> messages;

    public Map<NoticeType,String> getMessages() {
        return messages;
    }

    public Notice(final BackEnd backEnd, final Session session, final int size) {
        super(backEnd);

        final Map<NoticeType,String> map = new LinkedHashMap<>();
        byte byteType;
        while((byteType = session.get()) != NULL) {
            map.put(NoticeType.from(byteType), session.nullString());
        }

        this.messages = Collections.unmodifiableMap(map);
    }

    public final static ResponseBuilder builder = (final BackEnd backEnd, final int size, final Session session) -> {
        return new Notice(backEnd, session, size);
    };

    public void throwMe() {
        throw new PostgresqlException(messages);
    }
}
