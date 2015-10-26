package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.PostgresqlStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.BiPredicate;

public class Notice extends Response {

    final private Map<NoticeType,String> messages;

    public Map<NoticeType,String> getMessages() {
        return messages;
    }

    public Notice(final BackEnd backEnd, final PostgresqlStream stream, final int size) {
        super(backEnd, size);

        final Map<NoticeType,String> map = new LinkedHashMap<>();
        byte byteType;
        while((byteType = stream.get()) != NULL) {
            map.put(NoticeType.from(byteType), stream.nullString());
        }

        this.messages = Collections.unmodifiableMap(map);
    }

    public static BiPredicate<PostgresqlStream,Response> addMap(final Map<NoticeType,String> map) {
        return addMap(map, false);
    }
    
    public static Predicate<PostgresqlStream,Response> addMap(final Map<NoticeType,String> map,
                                                              final boolean shouldContinue) {
        return (s,r) -> {
            map.putAll(((Notice) r).getMessages());
            return shouldContinue;
        };
    }

    public static Errors errors() {
        return new Errors();
    }

    public static class Errors implements Predicate<Response> {
        
        private Map<NoticeType,String> cause;
        
        public boolean test(final PostgresqlStream s, final Response r) {
            this.cause = ((Notice) r).messages;
            return false;
        }

        public Map<NoticeType,String> getCause() {
            return cause;
        }

        public boolean getHasAny() {
            return cause != null;
        }

        public void reset() {
            cause = null;
        }
    }
}
