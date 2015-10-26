package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.PostgresqlStream;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

public class ParameterStatus extends Response {

    private final String name;
    private final String value;
    
    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public ParameterStatus(final PostgresqlStream stream, final int size) {
        super(BackEnd.ParameterStatus, size);
        this.name = stream.nullString();
        this.value = stream.nullString();
    }

    public static BiPredicate<PostgresqlStream,Response> addMap(final Map<String,String> map) {
        return (s,r) -> {
            ParameterStatus o (ParameterStatus) r;
            map.put(o.name, o.value);
            return true;
        };
    }
}
