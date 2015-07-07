package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class Notice extends Response {

    final private Map<NoticeType,String> messages;

    public Map<NoticeType,String> getMessages() {
        return messages;
    }

    public Notice(final Stream stream, final int size) {
        super(BackEnd.NoticeResponse);

        final Map<NoticeType,String> map = new LinkedHashMap<>();
        byte byteType;
        while((byteType = stream.get()) != NULL) {
            map.put(NoticeType.from(byteType), stream.nullString());
        }

        this.messages = Collections.unmodifiableMap(map);
    }

    public final static ResponseBuilder builder = (final BackEnd backEnd, final int size, final Stream stream) -> {
        return new Notice(stream, size);
    };

    public void throwMe() {
        throw new PostgresqlException(messages);
    }
}
