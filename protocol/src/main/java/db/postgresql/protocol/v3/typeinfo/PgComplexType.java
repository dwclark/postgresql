package db.postgresql.protocol.v3.typeinfo;

import java.util.List;
import java.util.Collections;

public class PgComplexType {

    public static class Key {
        final String database;
        final int relId;

        public Key(final String database, final int relId) {
            this.database = database;
            this.relId = relId;
        }

        @Override
        public boolean equals(Object rhs) {
            if(!(rhs instanceof Key)) {
                return false;
            }

            Key o = (Key) rhs;
            return database.equals(o.database) && relId == o.relId;
        }

        @Override
        public int hashCode() {
            return database.hashCode() + relId;
        }
    }
    
    private final Key key;
    private List<PgAttribute> attributes;

    public PgComplexType(final String database, final int relId, final List<PgAttribute> attributes) {
        this.key = new Key(database, relId);
        this.attributes = Collections.unmodifiableList(attributes);
    }

    public Key getKey() { return key; }
    public String getDatabase() { return key.database; }
    public int getRelId() { return key.relId; }
    public List<PgAttribute> getAttributes() { return attributes; }

    @Override
    public boolean equals(final Object rhs) {
        if(!(rhs instanceof PgComplexType)) {
            return false;
        }

        final PgComplexType o = (PgComplexType) rhs;
        return key.equals(o.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
}
