package db.postgresql.protocol.v3.typeinfo;

import db.postgresql.protocol.v3.Session;
import db.postgresql.protocol.v3.DataRow;
import db.postgresql.protocol.v3.SimpleQuery;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.List;

public class Registry {

    public static PgType pgType(final Session session, final int oid) {
        final PgType.OidKey key = new PgType.OidKey(session.getDatabase(), oid);
        final PgType ptype = pgTypes.get(key);
        if(ptype != null) {
            return ptype;
        }
        
        session.withDuplicateSession((Session dup) -> populatePgType(dup, oid));
        return pgTypes.get(key);
    }

    public static PgType pgType(final Session session, final String fullName) {
        final PgType.NameKey key = new PgType.NameKey(session.getDatabase(), fullName);
        final PgType ptype = pgTypesByName.get(key);
        if(ptype != null) {
            return ptype;
        }

        session.withDuplicateSession((Session dup) -> populatePgType(dup, fullName));
        return pgTypes.get(key);
    }

    public static PgComplexType pgComplexType(final String database, final int relId) {
        return pgComplexTypes.get(new PgComplexType.Key(database, relId));
    }

    public static PgComplexType pgComplexType(final Session session, final int relId) {
        final PgComplexType.Key key = new PgComplexType.Key(session.getDatabase(), relId);
        final PgComplexType pgComplexType = pgComplexTypes.get(key);
        if(pgComplexType != null) {
            return pgComplexType;
        }
        
        session.withDuplicateSession((Session dup) -> populatePgComplexType(dup, relId));
        return pgComplexTypes.get(key);
    }

    private static final Map<PgType.OidKey,PgType> pgTypes = new ConcurrentHashMap<>(100, 0.75f, 1);
    private static final Map<PgType.NameKey,PgType> pgTypesByName = new ConcurrentHashMap<>(100, 0.75f, 1);
    private static final Map<PgComplexType.Key,PgComplexType> pgComplexTypes = new ConcurrentHashMap<>(50, 0.75f, 1);

    private static void add(PgType pgType) {
        pgTypes.put(pgType.getOidKey(), pgType);
        pgTypesByName.put(pgType.getNameKey(), pgType);
    }
    
    private static void add(PgComplexType pgType) {
        pgComplexTypes.put(pgType.getKey(), pgType);
    }

    private static void populatePgType(final Session session, final int oid) {
        final String sql = String.format("select typ.oid, ns.nspname, typ.typname, typ.typarray, typ.typrelid from pg_type typ " +
                                         "join pg_namespace ns on typ.typnamespace = ns.oid where typ.oid = %d", oid);
        _populatePgType(session, sql);
    }

    private static void populatePgType(final Session session, final String fullName) {
        final PgType.NameKey nameKey = new PgType.NameKey(session.getDatabase(), fullName);
        if(nameKey.hasSchema()) {
            final String sql = String.format("select typ.oid, ns.nspname, typ.typname, typ.typarray, typ.typrelid from pg_type typ " +
                                             "join pg_namespace ns on typ.typnamespace = ns.oid where ns.nspname = '%s' " +
                                             "and typ.typname = '%s'", nameKey.getSchema(), nameKey.getName());
            _populatePgType(session, sql);
        }
        else {
            final String sql = String.format("select typ.oid, ns.nspname, typ.typname, typ.typarray, typ.typrelid from pg_type typ " +
                                             "join pg_namespace ns on typ.typnamespace = ns.oid where typ.typname = '%s'",
                                             nameKey.getName());
            _populatePgType(session, sql);
        }        
    }

    private static void _populatePgType(final Session session, final String sql) {
        final PgType pg = new SimpleQuery(sql, session).singleResult((DataRow.Iterator iter) -> {
                return new PgType(iter.nextInt(), iter.nextString(), iter.nextString(),
                                  iter.nextInt(), iter.nextInt()); });
        add(pg);
        
        if(!pg.isArray() && !pgTypes.containsKey(pg.getArrayId())) {
            populatePgType(session, pg.getArrayId());
        }
        
        if(pg.isComplex() && !pgComplexTypes.containsKey(pg.getRelId())) {
            populatePgComplexType(session, pg.getRelId());
        }
    }

    private static void populatePgComplexType(final Session session, final int relId) {
        final String sql = String.format("select attrelid, attname, atttypid, attnum " +
                                         "from pg_attribute where attrelid = %d", relId);
        final List<PgAttribute> attrs = new SimpleQuery(sql, session).manyResults((DataRow.Iterator iter) -> {
                return new PgAttribute(iter.nextInt(), iter.nextString(), iter.nextInt(), iter.nextInt()); });
        final PgComplexType pg = new PgComplexType(session.getDatabase(), relId, attrs);
        add(pg);
        
        for(PgAttribute pgAttr : attrs) {
            if(!pgTypes.containsKey(pgAttr.getTypeId())) {
                populatePgType(session, pgAttr.getTypeId());
            }
        }
    }

    static {
        //TODO: Load default types
    }
}
