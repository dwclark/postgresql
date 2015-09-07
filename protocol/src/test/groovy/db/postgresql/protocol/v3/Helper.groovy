package db.postgresql.protocol.v3;

class Helper {

    static def host = '127.0.0.1';
    static def port = 5432;
    static def database = 'testdb';

    static Session noAuth() {
        new Session.Builder().host(host).port(port).database(database).user('noauth').build();
    }
}
