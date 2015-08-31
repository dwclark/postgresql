package db.postgresql.protocol.v3.serializers;

import spock.lang.*;
import java.nio.*;

class UdtParserTest extends Specification {
    
    def "Complex UDT"() {
        setup:
        def toParse = '(11-01-1975,"David","Clark",23,"quote""","(""123 Main"""""",""Suite 100"",Fargo,90210,""(45,45)"")",)';
    }
}
