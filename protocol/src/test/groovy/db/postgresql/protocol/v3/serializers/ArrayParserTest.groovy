package db.postgresql.protocol.v3.serializers;

import spock.lang.*;
import java.nio.*;

class ArrayParserTest extends Specification {

    def intAry1 = '{1,2,3}';
    def varcharAry1 = "{foo,bar,baz}";
    def varcharAray1Quotes = "{'foo ','bar ','baz '}";
    def intAry3x3 = ('{{{1,2,3},{4,5,6},{7,8,9}},' +
                     '{{11,12,13},{14,15,16},{17,18,19}},' +
                     '{{21,22,23},{24,25,26},{27,28,29}}}');

}
