package db.postgresql.protocol.v3.typeinfo;

import db.postgresql.protocol.v3.Session;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.serializers.Serializer;
import java.util.List;

public class PgType {

    public static class OidKey {
        final String database;
        final int oid;

        public OidKey(final String database, final int oid) {
            this.database = database;
            this.oid = oid;
        }

        public OidKey(final OidKey toCopy) {
            this(toCopy.database, toCopy.oid);
        }

        @Override
        public boolean equals(final Object rhs) {
            if(!(rhs instanceof OidKey)) {
                return false;
            }

            OidKey o = (OidKey) rhs;
            return database.equals(o.database) && oid == o.oid;
        }

        @Override
        public int hashCode() {
            return oid + database.hashCode();
        }
    }

    public static class NameKey {
        final String database;
        final String schema;
        final String name;

        public NameKey(final String database, final String schema, final String name) {
            this.database = database;
            this.schema = filterSchema(schema);
            this.name = name;
        }

        public NameKey(final NameKey toCopy) {
            this(toCopy.database, toCopy.schema, toCopy.name);
        }

        public NameKey(final String database, final String fullName) {
            this.database = database;
            final String[] ary = fullName.split("\\.");
            if(ary.length == 1) {
                this.schema = "";
                this.name = ary[0];
            }
            else {
                this.schema = ary[0];
                this.name = ary[1];
            }
        }

        public boolean hasSchema() {
            return !"".equals(schema);
        }

        public String getFullName() {
            if(hasSchema()) {
                return schema + '.' + name;
            }
            else {
                return name;
            }
        }

        private static String filterSchema(String schema) {
            if(schema.equals("public") || schema.equals("pg_catalog")) {
                return "";
            }
            else {
                return schema;
            }
        }

        @Override
        public boolean equals(Object rhs) {
            if(!(rhs instanceof NameKey)) {
                return false;
            }

            NameKey o = (NameKey) rhs;
            return database.equals(o.database) && schema.equals(o.schema) && name.equals(o.name);
        }

        public String getDatabase() { return database; }
        public String getSchema() { return schema; }
        public String getName() { return name; }
    }

    public static final String DEFAULT_DB = "";
    public static final Serializer EMPTY = new Serializer() {
            public Object readObject(Stream stream, int size, Format format) {
                throw new UnsupportedOperationException();
            }
        };
    
    final private OidKey oidKey;
    final private NameKey nameKey;
    final private int arrayId;
    final private int relId;
    final private Serializer serializer;

    private PgType(final OidKey oidKey, final NameKey nameKey, final int arrayId,
                   final int relId, final Serializer serializer) {
        this.oidKey = oidKey;
        this.nameKey = nameKey;
        this.arrayId = arrayId;
        this.relId = relId;
        this.serializer = serializer;
    }

    public PgType(final String database, final int oid, final String schema,
                  final String name, final int arrayId, final int relId, final Serializer serializer) {
        this(new OidKey(database, oid), new NameKey(database, schema, name),
             arrayId, relId, serializer);
    }
    
    public PgType(final String database, final int oid, final String schema,
                  final String name, final int arrayId, final int relId) {
        this(database, oid, schema, name, arrayId, relId, EMPTY);
    }

    public PgType(final int oid, final String schema,
                  final String name, final int arrayId, final int relId) {
        this(DEFAULT_DB, oid, schema, name, arrayId, relId);
    }

    public OidKey getOidKey() { return oidKey; }
    public NameKey getNameKey() { return nameKey; }
    public String getDatabase() { return oidKey.database; }
    public int getOid() { return oidKey.oid; }
    public String getSchema() { return nameKey.schema; }
    public String getName() { return nameKey.name; }
    public int getArrayId() { return arrayId; }
    public int getRelId() { return relId; }
    public boolean isComplex() { return relId != 0; }
    public boolean isArray() { return arrayId == 0; };

    public String getFullName() { return nameKey.getFullName(); }

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

    public List<PgAttribute> getAttributes() {
        if(isComplex()) {
            return Registry.pgComplexType(getDatabase(), relId).getAttributes();
        }
        else {
            return null;
        }
    }
}

