package db.postgresql.protocol.v3.serializers;

import java.util.Deque;
import java.util.ArrayDeque;
import java.util.function.BiFunction;
import static db.postgresql.protocol.v3.serializers.ParserMeta.*;

//Rule: UDT consumes udt char and any quotes
//Rule: Field consumes quotes and content, but only makes content available
//Rule: Field will not consume anything more than one char if null field
//Rule: Field will leave index one past meta char (, or udt delimiter)
public class CompositeEngine {

    private final Deque<ParserMeta> levels = new ArrayDeque<>();
    private final BiFunction<Character,Integer,ParserMeta> factory;
    
    final CharSequence buffer;
    private int index;

    public CompositeEngine(final CharSequence buffer, final BiFunction<Character,Integer,ParserMeta> factory) {
        this.buffer = buffer;
        this.factory = factory;
    }

    public ParserMeta getLevel() {
        return levels.peek();
    }

    private static boolean allQuotes(final CharSequence seq) {
        for(int i = 0; i < seq.length(); ++i) {
            if(seq.charAt(i) != QUOTE) {
                return false;
            }
        }
        
        return true;
    }
    
    private static boolean anyQuotes(final CharSequence seq) {
        for(int i = 0; i < seq.length(); ++i) {
            if(seq.charAt(i) == QUOTE) {
                return true;
            }
        }

        return false;
    }

    private void advanceEndNoQuotes() {
        while(true) {
            final char current = buffer.charAt(index);
            if(isEnd(current) || isDiv(current)) {
                break;
            }

            ++index;
        }
    }

    private void advanceEndQuotes() {
        while(true) {
            final char current = buffer.charAt(index);
            final char possibleEnd = buffer.charAt(index + getLevel().getFieldQuotes());
            if(isQuote(current) && !isQuote(possibleEnd)) {
                break;
            }
            else {
                ++index;
            }
        }
    }
    
    public String getField() {
        boolean withinQuotes = false;
        if(isEnd(buffer.charAt(index))) {
            //case 1: at the end of the udt, no field present
            return null;
        }

        if(isDiv(buffer.charAt(index))) {
            //case 2: null field, but not at the end of the udt
            ++index;
            return null;
        }

        if(isQuote(buffer.charAt(index))) {
            //advance the the content, record that we are inside quotes
            index += getLevel().getFieldQuotes();
            withinQuotes = true;
        }

        //at this point there is definitely content and the index is pointed at the beginning
        final int contentBegin = index;
        if(withinQuotes) {
            advanceEndQuotes();
        }
        else {
            advanceEndNoQuotes();
        }

        final int contentEnd = index;

        if(withinQuotes) {
            index += getLevel().getFieldQuotes();
        }
        
        if(isDiv(buffer.charAt(index))) {
            //advance past comma to position next read on either the
            //beginning of the field boundary or at the udt end
            ++index;
        }

        //return the actual content
        CharSequence seq = buffer.subSequence(contentBegin, contentEnd);
        if(anyQuotes(seq)) {
            return getLevel().replace(seq.toString());
        }
        else {
            return seq.toString();
        }
    }

    public void beginUdt() {
        final ParserMeta tmp = factory.apply('(', levels.size());
        index += tmp.getUdtQuotes();
        final ParserMeta next = factory.apply(buffer.charAt(index++), levels.size());
        levels.push(next);
    }

    public void endUdt() {
        final ParserMeta last = levels.pop();
        assert(last.validEnd(buffer.charAt(index++)));
        index += last.getUdtQuotes();
    }

    public char getCurrent() {
        return buffer.charAt(index);
    }
}
