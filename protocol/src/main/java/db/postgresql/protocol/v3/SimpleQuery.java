package db.postgresql.protocol.v3;

import java.util.EnumSet;

public class SimpleQuery extends Query {

    public SimpleQuery(final String query, final Session stream) {
        super(stream);
        stream.query(query);
    }
}
