package db.postgresql.protocol.v3.typeinfo;

import db.postgresql.protocol.v3.Session;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class Registry {

    private static final Map<Integer,PgType> pgTypes = new ConcurrentHashMap<>(100, 0.75f, 1);
    private static final Map<Integer,PgComplexType> pgComplexTypes = new ConcurrentHashMap<>(50, 0.75f, 1);
    
    public static PgType pgType(final Session session, final int oid) {
        PgType ptype = pgTypes.get(oid);
        if(ptype == null) {
            session.withDuplicateSession((Session dup) -> populatePgType(dup, oid));
        }

        return pgTypes.get(oid);
    }

    public static PgComplexType pgComplexType(final Session session, final int relId) {
        PgComplexType pgComplexType = pgComplexTypes.get(relId);
        if(pgComplexType == null) {
            session.withDuplicateSession((Session dup) -> populatePgComplexType(dup, relId));
        }

        return pgComplexTypes.get(relId);
    }

    private static void populatePgType(final Session session, final int oid) {

    }

    private static void populatePgComplexType(final Session session, final int relId) {
        
    }
}
