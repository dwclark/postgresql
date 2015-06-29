package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class Notice extends Response {

    public Map<NoticeType,String> getMessages() {
        final Map<NoticeType,String> ret = new LinkedHashMap<>();
        for(int i = 0; i < buffer.limit(); ++i) {
            byte byteType = buffer.get(i++);
            if(byteType == NULL) {
                break;
            }
            
            NoticeType type = NoticeType.from(byteType);
            int next = nextNull(i);
            ret.put(type, nullString(i, next));
            i = next;
        }

        return ret;
    }

    public Notice() {
        super(BackEnd.NoticeResponse);
    }

    public Notice(Notice toCopy) {
        super(toCopy);
    }

    @Override
    public Notice copy() {
        return new Notice(this);
    }

    private static final ThreadLocal<Notice> tlData = new ThreadLocal<Notice>() {
            @Override protected Notice initialValue() {
                return new Notice();
            }
        };

    public final static ResponseBuilder builder = new ResponseBuilder() {

            public Notice build(final BackEnd backEnd, final int size, final Stream stream) {
                return (Notice) tlData.get().reset(stream.getRecord(size), stream.getEncoding());
            }
        };
}
