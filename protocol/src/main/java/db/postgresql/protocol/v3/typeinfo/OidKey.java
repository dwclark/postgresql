package db.postgresql.protocol.v3.typeinfo;

public abstract class OidKey {

    public abstract String getDatabase();
    public abstract int getOid();

    @Override
    public boolean equals(Object rhs) {
        if(this == rhs) {
            return true;
        }

        if(!(rhs instanceof OidKey)) {
            return false;
        }

        OidKey o = (OidKey) rhs;
        return (getDatabase().equals(o.getDatabase()) &&
                getOid() == o.getOid());
    }
    
    @Override
    public int hashCode() {
        return (getDatabase().hashCode() * 37) + (getOid() * 173);
    }
    
    private static class ImmutableOidKey extends OidKey {
        private final String database;
        private final int oid;

        private ImmutableOidKey(final String database, final int oid) {
            this.database = database;
            this.oid = oid;
        }

        public String getDatabase() { return database; }
        public int getOid() { return oid; }
    }
    
    private static class MutableOidKey extends OidKey {
        private String database;
        private int oid;

        public String getDatabase() { return database; }
        public int getOid() { return oid; }
    }

    private static final ThreadLocal<MutableOidKey> tlMutable = new ThreadLocal<MutableOidKey>() {
            @Override protected MutableOidKey initialValue() {
                return new MutableOidKey();
            }
        };
    
    public static OidKey immutable(final String database, final int oid) {
        return new ImmutableOidKey(database, oid);
    }

    public static OidKey threadLocal(final String database, final int oid) {
        MutableOidKey mut = tlMutable.get();
        mut.database = database;
        mut.oid = oid;
        return mut;
    }
}
