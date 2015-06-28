package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.util.Map;

public class ParameterStatus extends Response {

    public String getName() {
        return nullString(0, nextNull(0))
    }

    public String getValue() {
        final int startAt = nextNull(0) + 1;
        return nullString(startAt, nextNull(startAt));
    }

    private ParameterStatus() {
        super(BackEnd.ParameterStatus);
    }

    private ParameterStatus(ParameterStatus toCopy) {
        super(BackEnd.ParameterStatus, toCopy);xs
    }

    @Override
    public ParameterStatus copy() {
        return new ParameterStatus(this);
    }

    private static final ThreadLocal<ParameterStatus> tlData = new ThreadLocal<ParameterStatus>() {
            @Override protected ParameterStatus initialValue() {
                return new ParameterStatus();
            }
        };
    
    public final static ResponseBuilder builder = new ResponseBuilder() {
            public ParameterStatus build(final BackEnd backEnd, final int size, final Stream stream) {
                return (ParameterStatus) tlData.get().reset(stream.getRecord(size), stream.getEncoding());
            }
        };
}
