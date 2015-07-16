package db.postgresql.protocol.v3;

import spock.lang.*;
import java.nio.channels.*;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.security.*;
import static db.postgresql.protocol.v3.ssl.ContextCreation.*;

class QueryTest extends Specification {

    def host = '127.0.0.1';
    def port = 5432;
    def database = 'testdb';

    Session.Builder sb;

    def setup() {
        sb = new Session.Builder().host(host).port(port).database(database);
    }

    def processResults(final SimpleQuery sq) {
        Results results;
        while(results = sq.nextResults()) {
            if(results.resultType == ResultType.HAS_RESULTS) {
                Iterator<DataRow> rowIter = results.rows();
                while(rowIter.hasNext()) {
                    DataRow row = rowIter.next();
                    DataRow.Iterator colIter = row.iterator();
                    while(colIter.hasNext()) {
                        print(colIter.next() + ' ');
                    }
                    println();
                }
            }
            else if(results.resultType == ResultType.NO_RESULTS) {
                println("No Results");
            }
            else if(results.resultType == ResultType.EMPTY) {
                println("Empty Row!");
            }
            else {
                throw new UnsupportedOperationException();
            }
        }

        println("Transaction Status: ${sq.status}");
    }

    def "Empty Query"() {
        Session session = sb.user('noauth').build();
        SimpleQuery sq = new SimpleQuery('', session);
        processResults(sq);
        
        cleanup:
        session.close();
    }

    def "Row Process"() {
        setup:
        Session session = sb.user('noauth').build();
        SimpleQuery sq = new SimpleQuery('select * from items;', session);
        processResults(sq);
        
        cleanup:
        session.close();
    }

    def "Multi Row Process"() {
        setup:
        Session session = sb.user('noauth').build();
        SimpleQuery sq = new SimpleQuery('select * from items; ;select * from items;', session);
        processResults(sq);
        
        cleanup:
        session.close();
    }

    def "Row Array Proccess"() {
        setup:
        Session session = sb.user('noauth').build();
        SimpleQuery sq = new SimpleQuery('select * from items;', session);
        Results results;
        while(results = sq.nextResults()) {
            Iterator<DataRow> rowIter = results.rows();
            while(rowIter.hasNext()) {
                DataRow row = rowIter.next();
                Object[] ary = row.toArray();
                assert(ary.length == 2);
                println(ary);
            }
        }
    }

    def "Row Array All Basic Types"() {
        setup:
        Session session = sb.user('noauth').build();
        SimpleQuery sq = new SimpleQuery('select * from all_types;', session);
        Results results;
        while(results = sq.nextResults()) {
            Iterator<DataRow> rowIter = results.rows();
            while(rowIter.hasNext()) {
                DataRow row = rowIter.next();
                Object[] ary = row.toArray();
                assert(ary.length == 18);
                println(ary);
            }
        }
    }
}
