package db.postgresql.protocol.v3.typeinfo;

import java.util.Collections;
import java.util.SortedSet;

public class PgComplexType {

    private final OidKey oidKey;
    private SortedSet<PgAttribute> attributes;

    private PgComplexType(final OidKey oidKey, final SortedSet<PgAttribute> attributes) {
        this.oidKey = oidKey;
        this.attributes = Collections.unmodifiableSortedSet(attributes);
    }
    
    public PgComplexType(final String database, final int relId, final SortedSet<PgAttribute> attributes) {
        this(OidKey.immutable(database, relId), attributes);
    }

    public OidKey getOidKey() { return oidKey; }
    public String getDatabase() { return oidKey.getDatabase(); }
    public int getRelId() { return oidKey.getOid(); }
    public SortedSet<PgAttribute> getAttributes() { return attributes; }

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
