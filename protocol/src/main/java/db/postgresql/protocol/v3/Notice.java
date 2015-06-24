package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class Notice extends Response {

    private final Map<NoticeType,String> errors;

    public Notice(final BackEnd backEnd, final Map<NoticeType,String> errors) {
        super(backEnd);
        this.errors = Collections.unmodifiableMap(errors);
    }

    public final static ResponseBuilder builder = new ResponseBuilder() {

            public Notice build(final BackEnd backEnd, final int size, final Stream stream) {
                Map<NoticeType,String> errors = new LinkedHashMap<>();
                int left = size;
                while(left != 1) {
                    errors.put(NoticeType.from(stream.get()), stream.nullString());
                }

                stream.getNull();
                return new Notice(backEnd, errors);
            }
        };
}
