package db.postgresql.protocol.v3.serializers;

import spock.lang.*;
import java.nio.*;

class ArrayParserTest extends Specification {

    CharSequence intAry = '{1,2,3}';
    CharSequence varcharAry = "{foo,bar,baz}";
    CharSequence varcharAryQuotes = '{"foo ","bar ","baz "}';
    CharSequence intAry2x2 = '{{1,2},{3,4}}';
    CharSequence intAry3x3x3 = ('{{{1,2,3},{4,5,6},{7,8,9}},' +
                                '{{11,12,13},{14,15,16},{17,18,19}},' +
                                '{{21,22,23},{24,25,26},{27,28,29}}}');

    CharSequence ary4x1 = '{{1},{2},{3},{4}}';
    CharSequence ary1x1x4x1 = '{{{{1},{2},{3},{4}}}}';
    CharSequence ary5x2 = '{{1,2},{3,4},{1,2},{3,4},{1,2}}';

    def "Test Dimension Parsing"() {
        setup:
        StringSerializer sser = new StringSerializer(Serializer.ASCII_ENCODING);
        IntSerializer iser = IntSerializer.instance;
        char d = ',';
        int[] dimsIntAry = new ArrayParser(intAry, iser, d).dimensions;
        int[] dimsVarcharAry = new ArrayParser(varcharAry, sser, d).dimensions;
        int[] dimsVarcharAryQuotes = new ArrayParser(varcharAryQuotes, sser, d).dimensions;
        int[] dimsAry3x3x3 = new ArrayParser(intAry3x3x3, iser, d).dimensions;
        int[] dimsAry4x1 = new ArrayParser(ary4x1, iser, d).dimensions;
        int[] dimsAry1x1x4x1 = new ArrayParser(ary1x1x4x1, iser, d).dimensions;
        int[] dimsAry5x2 = new ArrayParser(ary5x2, iser, d).dimensions;
        
        expect:
        dimsIntAry.length == 1;
        dimsIntAry[0] == 3;
        dimsVarcharAry.length == 1;
        dimsVarcharAry[0] == 3;
        dimsVarcharAryQuotes.length == 1;
        dimsVarcharAryQuotes[0] == 3;
        dimsAry3x3x3.length == 3;
        dimsAry3x3x3.every { it == 3; };
        dimsAry4x1.length == 2;
        dimsAry4x1[0] == 4;
        dimsAry4x1[1] == 1;
        dimsAry1x1x4x1[0] == 1;
        dimsAry1x1x4x1[1] == 1;
        dimsAry1x1x4x1[2] == 4;
        dimsAry1x1x4x1[3] == 1;
        dimsAry5x2.length == 2;
        dimsAry5x2[0] == 5;
        dimsAry5x2[1] == 2;
    }
}
