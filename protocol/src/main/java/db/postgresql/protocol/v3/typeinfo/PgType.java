package db.postgresql.protocol.v3.typeinfo;

import db.postgresql.protocol.v3.Session;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import java.util.List;

public class PgType {

    public static final String DEFAULT_DB = "";
    public static final String DEFAULT_SCHEMA = "public";
    public static final int DEFAULT_RELID = 0;
    public static final char DEFAULT_DELIMITER = ',';
    
    final private OidKey oidKey;
    final private NameKey nameKey;
    final private OidKey arrayKey;
    final private int relId;
    final private char delimiter;

    private PgType(final OidKey oidKey, final OidKey arrayKey, final NameKey nameKey,
                   final int relId, final char delimiter) {
        this.oidKey = oidKey;
        this.arrayKey = arrayKey;
        this.nameKey = nameKey;
        this.relId = relId;
        this.delimiter = delimiter;
    }

    public static class Builder {
        private String database = DEFAULT_DB;
        private String schema = DEFAULT_SCHEMA;
        private String name;
        private int oid;
        private int arrayId;
        private int relId = DEFAULT_RELID;
        private char delimiter = DEFAULT_DELIMITER;

        public Builder database(final String val) { database = val; return this; }
        public Builder schema(final String val) { schema = val; return this; }
        public Builder name(final String val) { name = val; return this; }
        public Builder oid(final int val) { oid = val; return this; }
        public Builder arrayId(final int val) { arrayId = val; return this; }
        public Builder relId(final int val) { relId = val; return this; }
        public Builder delimiter(final char val) { delimiter = val; return this; }

        public PgType build() {
            return new PgType(OidKey.immutable(database, oid), OidKey.immutable(database, arrayId),
                              NameKey.immutable(database, schema, name), relId, delimiter);
        }

        public Builder() { }

        public Builder(final PgType pgType) {
            this.database = pgType.getDatabase();
            this.schema = pgType.getSchema();
            this.name = pgType.getName();
            this.oid = pgType.getOid();
            this.arrayId = pgType.getArrayId();
            this.relId = pgType.getRelId();
        }
    }

    public OidKey getOidKey() { return oidKey; }
    public OidKey getArrayKey() { return arrayKey; }
    public NameKey getNameKey() { return nameKey; }
    public String getDatabase() { return oidKey.getDatabase(); }
    public int getOid() { return oidKey.getOid(); }
    public String getSchema() { return nameKey.getSchema(); }
    public String getName() { return nameKey.getName(); }
    public int getArrayId() { return arrayKey.getOid(); }
    public int getRelId() { return relId; }
    public char getDelimiter() { return delimiter; }
    public boolean isComplex() { return relId != 0; }
    public String getFullName() { return nameKey.getFullName(); }
    public boolean isBuiltin() { return nameKey.isBuiltin(); } 

    @Override
    public boolean equals(Object rhs) {
        if(!(rhs instanceof PgType)) {
            return false;
        }

        PgType o = (PgType) rhs;
        return oidKey.equals(o.oidKey);
    }

    @Override
    public int hashCode() {
        return oidKey.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(256);
        sb.append("PgType(");
        sb.append("database: " + getDatabase() + ", ");
        sb.append("schema: " + getSchema() + ", ");
        sb.append("name: " + getName() + ", ");
        sb.append("fullName: " + getFullName() + ", ");
        sb.append("arrayId: " + getArrayId() + ", ");
        sb.append("relId: " + getRelId() + ", ");
        sb.append("delimiter: '" + getDelimiter() + "', ");
        sb.append("complex: " + isComplex() + ", ");
        sb.append("builtin: " + isBuiltin() + ", ");
        sb.append("attributes: " + getAttributes() + ")"); 
        return sb.toString();
    }

    public List<PgAttribute> getAttributes() {
        if(isComplex()) {
            return Registry.pgComplexType(getDatabase(), relId).getAttributes();
        }
        else {
            return null;
        }
    }
}

