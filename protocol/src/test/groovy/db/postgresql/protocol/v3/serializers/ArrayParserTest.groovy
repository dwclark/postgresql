package db.postgresql.protocol.v3.serializers;

import spock.lang.*;
import java.nio.*;
import db.postgresql.protocol.v3.types.Box;
import db.postgresql.protocol.v3.types.Point;
import db.postgresql.protocol.v3.Session;
import db.postgresql.protocol.v3.Helper;

class ArrayParserTest extends Specification {

    @Shared Session session;

    def setupSpec() {
        session = Helper.noAuth();
    }

    def cleanupSpec() {
        session.close();
    }
    
    StringSerializer sser = new StringSerializer(Serializer.ASCII_ENCODING);
    IntSerializer iser = IntSerializer.instance;
    char d = ',';
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

    def "Test To Array"() {
        when:
        int[] ary1 = new ArrayParser(intAry, iser, d).toArray();
        then:
        ary1 == [ 1, 2, 3 ] as int[];

        when:
        String[] ary2 = new ArrayParser(varcharAry, sser, d).toArray();
        then:
        ary2 == [ 'foo', 'bar', 'baz' ] as String[];

        when:
        String[] ary3 = new ArrayParser(varcharAryQuotes, sser, d).toArray();
        then:
        ary3 == [ "foo ","bar ","baz " ] as String[];

        when:
        int[][][] ary4 = new ArrayParser(intAry3x3x3, iser, d).toArray();
        then:
        ary4[0][0][0] == 1;
        ary4[0][2][2] == 9;
        ary4[1][1][1] == 15;
        ary4[2][2][2] == 29;
    }

    def "Test Arrays With Nulls"() {
        when:
        String[] ary = new ArrayParser('{"one",null,"null"}', sser, d).toArray();
        then:
        ary == [ 'one', null, 'null' ] as String[];
    }

    def "Test Box Array"() {
        when:
        Box[] ary = new ArrayParser('{(1,1),(0,0);(1,1),(-1.1,-1.1)}', new BoxSerializer(session),
                                    Box.PGTYPE.getDelimiter()).toArray();
        then:
        ary[0] == new Box(new Point(1,1), new Point(0,0));
        ary[1] == new Box(new Point(1,1), new Point(-1.1,-1.1));
    }
    
    def "Test Dimension Parsing"() {
        setup:
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

    def "Test Allocate Mods Single"() {
        when:
        int[] singleMod = ArrayParser.mods([ 1 ] as int[]);
        int[] singleIndexes = [ 0 ] as int[];
        ArrayParser.calculateIndexes(singleMod, singleIndexes, 0);
        
        then:
        singleMod[0] == 1;
        singleIndexes[0] == 0;

        when:
        singleMod = ArrayParser.mods([9] as int[]);
        singleIndexes = [ 0 ] as int[];
        ArrayParser.calculateIndexes(singleMod, singleIndexes, 3);
        
        then:
        singleIndexes[0] == 3;

        when:
        ArrayParser.calculateIndexes(singleMod, singleIndexes, 7);

        then:
        singleIndexes[0] == 7;
    }

    def "Test Allocate Mods 2x8"() {
        when:
        int[] mods = ArrayParser.mods([2,8] as int[]);
        int[] indexes = [ 0, 0 ] as int[];
        ArrayParser.calculateIndexes(mods, indexes, 8);
        then:
        indexes[0] == 1;
        indexes[1] == 0;

        when:
        ArrayParser.calculateIndexes(mods, indexes, 0);
        then:
        indexes[0] == 0;
        indexes[1] == 0;

        when:
        ArrayParser.calculateIndexes(mods, indexes, 15);
        then:
        indexes[0] == 1;
        indexes[1] == 7;
    }

    def "Test Allocate Mods 4x2x3"() {
        when:
        int[] mods = ArrayParser.mods([4,2,3] as int[]);
        int[] indexes = [ 0, 0, 0 ] as int[];
        ArrayParser.calculateIndexes(mods, indexes, 0);
        then:
        indexes == [ 0, 0, 0 ] as int[];

        when:
        ArrayParser.calculateIndexes(mods, indexes, 5)
        then:
        indexes == [ 0, 1, 2 ] as int[];

        when:
        ArrayParser.calculateIndexes(mods, indexes, 6);
        then:
        indexes == [ 1, 0, 0 ] as int[];

        when:
        ArrayParser.calculateIndexes(mods, indexes, 15);
        then:
        indexes == [ 2, 1, 0 ] as int[];

        when:
        ArrayParser.calculateIndexes(mods, indexes, 23);
        then:
        indexes == [ 3, 1, 2 ] as int[];
    }
}
