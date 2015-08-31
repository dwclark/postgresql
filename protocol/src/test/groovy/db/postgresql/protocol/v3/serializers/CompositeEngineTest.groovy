package db.postgresql.protocol.v3.serializers;

import spock.lang.*;
import java.nio.*;

class CompositeEngineTest extends Specification {

    def "Simple Test"() {
        setup:
        String buffer = '(1,1)';
        CompositeEngine engine = new CompositeEngine(buffer, ParserMeta.udt);
        engine.beginUdt();
        String one = engine.field;
        String two = engine.field;
        engine.endUdt();

        expect:
        one == '1';
        two == '1';
    }

    def "Simple Quoted Test"() {
        setup:
        String buffer = '("David","The Terrible")';
        CompositeEngine engine = new CompositeEngine(buffer, ParserMeta.udt);
        engine.beginUdt();
        String one = engine.field;
        String two = engine.field;
        engine.endUdt();

        expect:
        one == 'David';
        two == 'The Terrible';
    }

    def "Simple Null Field Test"() {
        setup:
        String buffer = '(,)';
        CompositeEngine engine = new CompositeEngine(buffer, ParserMeta.udt);
        engine.beginUdt();
        String one = engine.field;
        String two = engine.field;

        expect:
        one == null;
        two == null;
    }

    def "Simple Embedded Quote Test"() {
        setup:
        String buffer = '("David","""The Terrible""")';
        CompositeEngine engine = new CompositeEngine(buffer, ParserMeta.udt);
        engine.beginUdt();
        String one = engine.field;
        String two = engine.field;

        expect:
        one == 'David';
        two == '"The Terrible"';
    }

    def "Embedded Type Test"() {
        setup:
        String buffer = '("David","Clark","(""123 Main Street"",""El Cajon"",CA,93021)")'
        CompositeEngine engine = new CompositeEngine(buffer, ParserMeta.udt);
        engine.beginUdt();
        String firstName = engine.field;
        String lastName = engine.field;
        engine.beginUdt();
        String street = engine.field;
        String city = engine.field;
        String state = engine.field;
        String postal = engine.field;
        engine.endUdt();
        engine.endUdt();

        expect:
        firstName == 'David';
        lastName == 'Clark';
        street == '123 Main Street';
        city == 'El Cajon';
        state == 'CA';
        postal == '93021';
    }

    def "Complex Embedded Test"() {
        setup:
        String buffer = '(11-01-1975,"David","Clark",23,"quote""","(""123 Main"""""",""Suite 100"",Fargo,90210,""(45,45)"")",)';
        CompositeEngine engine = new CompositeEngine(buffer, ParserMeta.udt);
        engine.beginUdt();
        def person = [ birthDate: engine.field, firstName: engine.field, lastName: engine.field,
                       number: engine.field, misc: engine.field ];
        engine.beginUdt();
        def address = [ street1: engine.field, street2: engine.field, city: engine.field,
                        postal: engine.field ];
        engine.beginUdt();
        def latLong = [ latitude: engine.field, longitude: engine.field ];
        engine.endUdt();
        engine.endUdt();
        person.shouldBeNull = engine.field;
        engine.endUdt();

        expect:
        person.firstName == 'David';
        person.shouldBeNull == null;
        address.street1 == '123 Main"';
        address.postal == '90210';
    }
}
