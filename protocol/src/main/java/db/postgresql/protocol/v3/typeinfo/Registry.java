package db.postgresql.protocol.v3.typeinfo;

import db.postgresql.protocol.v3.Session;
import db.postgresql.protocol.v3.DataRow;
import db.postgresql.protocol.v3.SimpleQuery;
import db.postgresql.protocol.v3.serializers.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.List;

public class Registry {

    public static PgType pgType(final Session session, final int oid) {
        final PgType ptype = pgTypes.get(OidKey.threadLocal(session.getDatabase(), oid));
        if(ptype != null) {
            return ptype;
        }
        
        session.withDuplicateSession((Session dup) -> populatePgType(dup, oid));
        return pgTypes.get(OidKey.threadLocal(session.getDatabase(), oid));
    }

    public static PgType pgType(final Session session, final String fullName) {
        final PgType ptype = pgTypesByName.get(NameKey.threadLocal(session.getDatabase(), fullName));
        if(ptype != null) {
            return ptype;
        }

        session.withDuplicateSession((Session dup) -> populatePgType(dup, fullName));
        return pgTypesByName.get(NameKey.threadLocal(session.getDatabase(), fullName));
    }

    public static PgComplexType pgComplexType(final String database, final int relId) {
        return pgComplexTypes.get(OidKey.threadLocal(database, relId));
    }

    public static PgComplexType pgComplexType(final Session session, final int relId) {
        final PgComplexType pgComplexType = pgComplexTypes.get(OidKey.threadLocal(session.getDatabase(), relId));
        if(pgComplexType != null) {
            return pgComplexType;
        }
        
        session.withDuplicateSession((Session dup) -> populatePgComplexType(dup, relId));
        return pgComplexTypes.get(OidKey.threadLocal(session.getDatabase(), relId));
    }

    public static Serializer serializer(final String database, final int oid) {
        return databaseSerializers(database).get(oid);
    }

    @SuppressWarnings("unchecked")
    public static <T> Serializer<T> serializer(final String database, final Class<T> type) {
        final Map<Class,Serializer> tmp = typeSerializers(database);
        return (Serializer<T>) tmp.get(type);
    }

    private static Map<Integer,Serializer> databaseSerializers(final String database) {
        Map<Integer,Serializer> ret = databaseSerializers.get(database);
        if(ret == null) {
            databaseSerializers.putIfAbsent(database, new ConcurrentHashMap<>(100, 0.75f, 1));
            ret = databaseSerializers.get(database);
        }

        return ret;
    }

    private static Map<Class,Serializer> typeSerializers(final String database) {
        Map<Class,Serializer> ret = typeSerializers.get(database);
        if(ret == null) {
            typeSerializers.putIfAbsent(database, new ConcurrentHashMap<>(100, 0.75f, 1));
            ret = typeSerializers.get(database);
        }

        return ret;
    }

    public static void add(final PgType pgType) {
        pgTypes.put(pgType.getOidKey(), pgType);
        pgTypes.put(pgType.getArrayKey(), pgType);
        pgTypesByName.put(pgType.getNameKey(), pgType);
    }
    
    public static void add(final PgComplexType pgType) {
        pgComplexTypes.put(pgType.getOidKey(), pgType);
    }

    public static void add(final PgType pgType, final Serializer serializer) {
        final Map<Integer,Serializer> map = databaseSerializers(pgType.getOidKey().getDatabase());
        map.put(pgType.getOidKey().getOid(), serializer);
        map.put(pgType.getArrayKey().getOid(), serializer);
        add(pgType.getOidKey().getDatabase(), serializer);
    }

    public static void add(final String database, final Serializer serializer) {
        final Map<Class,Serializer> map = typeSerializers(database);
        for(Class type : serializer.getTypes()) {
            map.put(type, serializer);
        }
    }

    private static final Map<OidKey,PgType> pgTypes = new ConcurrentHashMap<>(100, 0.75f, 1);
    private static final Map<NameKey,PgType> pgTypesByName = new ConcurrentHashMap<>(100, 0.75f, 1);
    private static final Map<OidKey,PgComplexType> pgComplexTypes = new ConcurrentHashMap<>(50, 0.75f, 1);
    private static final Map<String,Map<Integer,Serializer>> databaseSerializers = new ConcurrentHashMap<>(5, 0.75f, 1);
    private static final Map<String,Map<Class,Serializer>> typeSerializers = new ConcurrentHashMap<>(5, 0.75f, 1);

    private static void populatePgType(final Session session, final int oid) {
        final String sql = String.format("select typ.oid, ns.nspname, typ.typname, typ.typarray, typ.typrelid from pg_type typ " +
                                         "join pg_namespace ns on typ.typnamespace = ns.oid where typ.oid = %d", oid);
        _populatePgType(session, sql);
    }

    private static void populatePgType(final Session session, final String fullName) {
        final NameKey nameKey = NameKey.immutable(session.getDatabase(), fullName);
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
                PgType.Builder builder = new PgType.Builder().database(session.getDatabase());
                builder.oid(iter.nextInt()).schema(iter.next(String.class)).name(iter.next(String.class));
                builder.arrayId(iter.nextInt()).relId(iter.nextInt());
                return builder.build(); });
        
        add(pg);
        
        if(pg.isComplex() && !pgComplexTypes.containsKey(pg.getRelId())) {
            populatePgComplexType(session, pg.getRelId());
        }
    }

    private static void populatePgComplexType(final Session session, final int relId) {
        final String sql = String.format("select attrelid, attname, atttypid, attnum " +
                                         "from pg_attribute where attrelid = %d and attnum >= 1", relId);
        final List<PgAttribute> attrs = new SimpleQuery(sql, session).manyResults((DataRow.Iterator iter) -> {
                return new PgAttribute(iter.nextInt(), iter.next(String.class), iter.nextInt(), iter.nextInt()); });
        final PgComplexType pg = new PgComplexType(session.getDatabase(), relId, attrs);
        
        add(pg);
        
        for(PgAttribute pgAttr : attrs) {
            if(!pgTypes.containsKey(OidKey.threadLocal(session.getDatabase(), pgAttr.getTypeId()))) {
                populatePgType(session, pgAttr.getTypeId());
            }
        }
    }
}
