package db.postgresql.protocol.v3.typeinfo;

import spock.lang.*;
import db.postgresql.protocol.v3.*;

class RegistryTest extends Specification {

    @Shared Session session;
    
    def setupSpec() {
        session = Helper.noAuth();
    }

    def cleanupSpec() {
        session.close();
    }

    def "Test Basic Type Load"() {
        setup:
        PgType pgType = Registry.pgType(session, 'public.items');
        PgType pgType2 = Registry.pgType(session, 'items');
        PgType pgType3 = Registry.pgType(session, pgType.getOid());
        println(pgType);
        
        expect:
        pgType;
        pgType.fullName == 'items';
        pgType == pgType2;
        pgType2 == pgType3;
        pgType.complex;
        pgType.attributes.size() == 2;
        pgType.delimiter == ',';
    }

}
