package db.postgresql.protocol.v3.typeinfo;

import java.util.List;
import java.util.Collections;

public class PgComplexType {

    private final OidKey oidKey;
    private List<PgAttribute> attributes;

    private PgComplexType(final OidKey oidKey, final List<PgAttribute> attributes) {
        this.oidKey = oidKey;
        this.attributes = Collections.unmodifiableList(attributes);
    }
    
    public PgComplexType(final String database, final int relId, final List<PgAttribute> attributes) {
        this(OidKey.immutable(database, relId), attributes);
    }

    public OidKey getOidKey() { return oidKey; }
    public String getDatabase() { return oidKey.getDatabase(); }
    public int getRelId() { return oidKey.getOid(); }
    public List<PgAttribute> getAttributes() { return attributes; }

    @Override
    public boolean equals(final Object rhs) {
        if(!(rhs instanceof PgComplexType)) {
            return false;
        }

        final PgComplexType o = (PgComplexType) rhs;
        return oidKey.equals(o.oidKey);
    }

    @Override
    public int hashCode() {
        return oidKey.hashCode();
    }
}
