package db.postgresql.protocol.v3.typeinfo;

import java.util.List;
import java.util.Collections;

public class PgComplexType {

    private final int relId;
    private List<PgAttribute> attributes;

    public PgComplexType(final int relId, final List<PgAttribute> attributes) {
        this.relId = relId;
        this.attributes = Collections.unmodifiableList(attributes);
    }

    public int getRelId() { return relId; }
    public List<PgAttribute> getAttributes() { return attributes; }

    @Override
    public boolean equals(final Object rhs) {
        if(!(rhs instanceof PgComplexType)) {
            return false;
        }

        final PgComplexType o = (PgComplexType) rhs;
        return relId == o.relId && attributes.equals(o.attributes); 
    }

    @Override
    public int hashCode() {
        return relId + attributes.hashCode();
    }
}
