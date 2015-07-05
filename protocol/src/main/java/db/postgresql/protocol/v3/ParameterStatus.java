package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
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

    private ParameterStatus(final Stream stream) {
        super(BackEnd.ParameterStatus);
        this.name = stream.nullString();
        this.value = stream.nullString();
    }

    public final static ResponseBuilder builder = new ResponseBuilder() {
            public ParameterStatus build(final BackEnd backEnd, final int size, final Stream stream) {
                return new ParameterStatus(stream);
            }
        };
}
