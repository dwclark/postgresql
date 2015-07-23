package db.postgresql.protocol.v3.typeinfo;

public abstract class NameKey {

    public abstract String getDatabase();
    public abstract String getSchema();
    public abstract String getName();

    @Override
    public boolean equals(Object rhs) {
        if(!(rhs instanceof NameKey)) {
            return false;
        }

        NameKey o = (NameKey) rhs;
        return (getDatabase().equals(o.getDatabase()) &&
                getSchema().equals(o.getSchema()) &&
                getName().equals(o.getName()));
    }

    @Override
    public int hashCode() {
        return ((getDatabase().hashCode() * 37) +
                (getSchema().hashCode() * 173) +
                (getName().hashCode() * 17));
    }

    public boolean hasSchema() {
        return !"".equals(getSchema());
    }
    
    public String getFullName() {
        if(hasSchema()) {
            return getSchema() + '.' + getName();
        }
        else {
            return getName();
        }
    }

    public boolean isBuiltin() {
        return "".equals(getDatabase()) && "".equals(getSchema());
    }

    private static String filterSchema(String schema) {
        if(schema.equals("public") || schema.equals("pg_catalog")) {
            return "";
        }
        else {
            return schema;
        }
    }

    private static class Immutable extends NameKey {
        private final String database;
        private final String schema;
        private final String name;

        public Immutable(final String database, final String schema, final String name) {
            this.database = database;
            this.schema = filterSchema(schema);
            this.name = name;
        }

        public String getDatabase() { return database; }
        public String getSchema() { return schema; }
        public String getName() { return name; }
    }

    private static class Mutable extends NameKey {
        private String database;
        private String schema;
        private String name;
        
        public String getDatabase() { return database; }
        public String getSchema() { return schema; }
        public String getName() { return name; }
    }

    private static final ThreadLocal<Mutable> tlMutable = new ThreadLocal<Mutable>() {
            @Override protected Mutable initialValue() {
                return new Mutable();
            }
        };

    public static NameKey immutable(final String database, final String schema, final String name) {
        return new Immutable(database, filterSchema(schema), name);
    }

    public static NameKey immutable(final String database, final String fullName) {
        final String[] ary = fullName.split("\\.");
        if(ary.length == 1) {
            return immutable(database, "", ary[0]);
        }
        else {
            return immutable(database, ary[0], ary[1]);
        }
    }

    public static NameKey threadLocal(final String database, final String schema, final String name) {
        Mutable mut = tlMutable.get();
        mut.database = database;
        mut.schema = schema;
        mut.name = name;
        return mut;
    }

    public static NameKey threadLocal(final String database, final String fullName) {
        final String[] ary = fullName.split("\\.");
        if(ary.length == 1) {
            return threadLocal(database, "", ary[0]);
        }
        else {
            return threadLocal(database, ary[0], ary[1]);
        }
    }
}
