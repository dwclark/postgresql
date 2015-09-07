package db.postgresql.protocol.v3;

import spock.lang.*;
import db.postgresql.protocol.v3.serializers.*;

class PrepareAndBindTest extends Specification {

    @Shared Session session;
    
    def setupSpec() {
        session = Helper.noAuth();
    }

    def cleanupSpec() {
        session.close();
    }

    def "Test Parse"() {
        setup:
        session.parse('_1', 'select * from items;', [] as int[]);
        session.sync();
        Response r = session.next(BackEnd.QUERY);
        println(r.backEnd);

        expect:
        r;
    }

    def "Test Multi Bind"() {
        setup:
        def NUM = 3;
        String insertName = 'insertIt';
        String deleteName = 'deleteIt';

        def willHandle = EnumSet.of(BackEnd.RowDescription, BackEnd.EmptyQueryResponse, BackEnd.ReadyForQuery,
                                    BackEnd.CommandComplete, BackEnd.DataRow, BackEnd.ParseComplete, BackEnd.BindComplete);
        session.parse(insertName, 'insert into items values ($1, $2)', Session.EMPTY_OIDS);
        
        IntSerializer intSerializer = IntSerializer.instance;
        StringSerializer strSerializer = session.stringSerializer;
        
        NUM.times { 
            Bindable[] inputs = [ intSerializer.bindable(3), strSerializer.bindable("three") ] as Bindable[];
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
    }

    def "Test Extended Query"() {
        setup:
        ExtendedQuery eq = session.extended('insert into items values ($1, $2)');
        eq.execute([ [ eq.bind(3), eq.bind('three') ] as Bindable[] ]);
        eq.noResults();
    }

    def "Test Multi Extended Query"() {
        setup:
        ExtendedQuery eq = session.extended('insert into items values ($1, $2)');
        List<Bindable[]> bindings = [ [ eq.bind(3), eq.bind('three') ] as Bindable[],
                                      [ eq.bind(4), eq.bind('four') ] as Bindable[],
                                      [ eq.bind(5), eq.bind('five') ] as Bindable[],
                                      [ eq.bind(6), eq.bind('six') ] as Bindable[] ];
        eq.execute(bindings);
        eq.noResults();

        ExtendedQuery eqDelete = session.extended('delete from items where id >= $1');
        eqDelete.execute([ [ eqDelete.bind(3) ] as Bindable[] ]);
        eqDelete.noResults();
    }
}

