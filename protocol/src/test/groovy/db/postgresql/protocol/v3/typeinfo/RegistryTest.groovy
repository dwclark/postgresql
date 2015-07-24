package db.postgresql.protocol.v3.typeinfo;

import spock.lang.*;
import db.postgresql.protocol.v3.*;

class RegistryTest extends Specification {

    def host = '127.0.0.1';
    def port = 5432;
    def database = 'testdb';

    Session.Builder sb;

    def setup() {
        sb = new Session.Builder().host(host).port(port).database(database);
    }

    def "Test Basic Type Load"() {
        setup:
        Session session = sb.user('noauth').build();
        PgType pgType = Registry.pgType(session, 'public.items');
        PgType pgType2 = Registry.pgType(session, 'items');
        PgType pgType3 = Registry.pgType(session, pgType.getOid());
        
        expect:
        pgType;
        pgType.fullName == 'items';
        pgType == pgType2;
        pgType2 == pgType3;
        pgType.complex;
        pgType.attributes.size() == 2;
    }
}
