package db.postgresql.protocol.v3;

import java.util.Map;

public class ParameterStatus extends Response {

    private final String name;
    private final String value;
    
    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    private ParameterStatus(final String name, final String value) {
        super(BackEnd.ParameterStatus);
        this.name = name;
        this.value = value;
    }

    public final static ResponseBuilder builder = new ResponseBuilder() {
            public ParameterStatus build(final BackEnd backEnd, final int size, final Session session) {
                return new ParameterStatus(session.nullString(), session.nullString());
            }
        };
}
