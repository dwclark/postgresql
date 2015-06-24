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

    public ParameterStatus(final BackEnd backEnd, final String name, final String value) {
        super(backEnd);
        this.name = name;
        this.value = value;
    }
    
    public final static ResponseBuilder builder = new ResponseBuilder() {
            public ParameterStatus build(final BackEnd backEnd, final int size, final Stream stream) {
                byte[] bytes = stream.get(new byte[size]);
                Map.Entry<String,String> pair = PostgresqlStream.nullPair(bytes, stream.getEncoding());
                return new ParameterStatus(backEnd, pair.getKey(), pair.getValue());
            }
        };
}
