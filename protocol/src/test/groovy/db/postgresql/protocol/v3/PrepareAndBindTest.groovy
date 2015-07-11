package db.postgresql.protocol.v3;

import spock.lang.*;
import java.nio.channels.*;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.security.*;
import static db.postgresql.protocol.v3.ssl.ContextCreation.*;
import db.postgresql.protocol.v3.serializers.*;

class PrepareAndBindTest extends Specification {

    def host = '127.0.0.1';
    def port = 5432;
    def database = 'testdb';

    Session.Builder sb;

    def setup() {
        sb = new Session.Builder().host(host).port(port).database(database);
    }

    def "Test Parse"() {
        setup:
        Session session = sb.user('noauth').build();
        session.parse('_1', 'select * from items;', [] as int[]);
        session.sync();
        Response r = session.next(BackEnd.QUERY);
        println(r.backEnd);

        expect:
        r;

        cleanup:
        session.close();
    }

    def "Test Multi Bind"() {
        setup:
        def NUM = 3;
        Session session = sb.user('noauth').build();
        String insertName = 'insertIt';
        String deleteName = 'deleteIt';

        def willHandle = EnumSet.of(BackEnd.RowDescription, BackEnd.EmptyQueryResponse, BackEnd.ReadyForQuery,
                                    BackEnd.CommandComplete, BackEnd.DataRow, BackEnd.ParseComplete, BackEnd.BindComplete);                                    
        session.parse(insertName, 'insert into items values ($1, $2)', Session.EMPTY_OIDS);
        //session.sync();
        
        
        IntSerializer intSerializer = session.serializer(IntSerializer);
        StringSerializer strSerializer = session.serializer(StringSerializer);
        NUM.times { 
            Bindable[] inputs = [ intSerializer.bindable(3, Format.TEXT), strSerializer.bindable("three", Format.TEXT) ] as Bindable[];
            session.bind('', insertName, inputs, Session.EMPTY_FORMATS).execute('');
        }
        
        session.sync();
        Response r = session.next(willHandle);
        println(r.backEnd); //parse complete

        NUM.times {
            r = session.next(willHandle);
            println(r.backEnd); //bind complete
            r = session.next(willHandle);
            println(r.backEnd); //command completed
        }

        r = session.next(willHandle);
        println(r.backEnd); //ready for query
        
        cleanup:
        session.close();
    }

    def processResults(final ExtendedQuery sq) {
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


    def "Test Extended Query"() {
        setup:
        Session session = sb.user('noauth').build();
        ExtendedQuery eq = new ExtendedQuery('insert into items values ($1, $2)', session);
        eq.execute([ [ eq.bind(3), eq.bind('three') ] as Bindable[] ]);
        processResults(eq);
    }

    def "Test Multi Extended Query"() {
        setup:
        Session session = sb.user('noauth').build();
        ExtendedQuery eq = new ExtendedQuery('insert into items values ($1, $2)', session);
        List<Bindable[]> bindings = [ [ eq.bind(3), eq.bind('three') ] as Bindable[],
                                      [ eq.bind(4), eq.bind('four') ] as Bindable[],
                                      [ eq.bind(5), eq.bind('five') ] as Bindable[],
                                      [ eq.bind(6), eq.bind('six') ] as Bindable[] ];
        eq.execute(bindings);

        ExtendedQuery eqDelete = new ExtendedQuery('delete from items where id >= $1', session);
        eqDelete.execute([ [ eqDelete.bind(3) ] as Bindable[] ]);
        processResults(eq);
    }
}

