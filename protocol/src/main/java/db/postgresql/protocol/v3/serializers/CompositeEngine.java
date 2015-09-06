package db.postgresql.protocol.v3.serializers;

import java.util.Deque;
import java.util.ArrayDeque;
import java.util.function.BiFunction;
import static db.postgresql.protocol.v3.serializers.CompositeMeta.*;

//Rule: UDT consumes udt char and any quotes
//Rule: Field consumes quotes and content, but only makes content available
//Rule: Field will not consume anything more than one char if null field
//Rule: Field will leave index one past meta char (, or udt delimiter)
public class CompositeEngine <T extends CompositeMeta> {

    final Deque<T> levels = new ArrayDeque<>();
    private final BiFunction<Character,Integer,T> factory;
    
    final CharSequence buffer;
    private int index = 0;
    private int contentBegin = -1;
    private int contentEnd = -1;
    private boolean withinQuotes = false;

    public int getIndex() {
        return index;
    }

    public int getContentBegin() {
        return contentBegin;
    }

    public int getContentEnd() {
        return contentEnd;
    }

    public boolean getWithinQuotes() {
        return withinQuotes;
    }
    
    public CompositeEngine(final CharSequence buffer, final BiFunction<Character,Integer,T> factory) {
        this.buffer = buffer;
        this.factory = factory;
    }

    public T getLevel() {
        return levels.peek();
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
        T level = getLevel();
        while(true) {
            final char current = buffer.charAt(index);
            if(level.isEnd(current) || level.isDelimiter(current)) {
                break;
            }

            ++index;
        }
    }

    private void advanceEndQuotes() {
        T level = getLevel();
        while(true) {
            final char current = buffer.charAt(index);
            if(isQuote(current)) {
                if(level.isEmbeddedQuotes(index, buffer)) {
                    index += level.getEmbeddedQuotes();
                }
                else {
                    break;
                }
            }
            else {
                ++index;
            }
        }
    }

    public void reset() {
        resetBoundaries();
        levels.clear();
        index = 0;
    }
    
    private void resetBoundaries() {
        contentBegin = -1;
        contentEnd = -1;
        withinQuotes = false;
    }
    
    public void findBoundaries() {
        resetBoundaries();
        T level = getLevel();
        
        if(level.isEnd(buffer.charAt(index))) {
            //case 1: at the end of the udt, no field present
            return;
        }

        if(level.isDelimiter(buffer.charAt(index))) {
            //case 2: null field, but not at the end of the udt
            ++index;
            return;
        }

        if(isQuote(buffer.charAt(index))) {
            //advance the the content, record that we are inside quotes
            index += level.getFieldQuotes();
            withinQuotes = true;
        }

        //at this point there is definitely content and the index is pointed at the beginning
        contentBegin = index;
        if(withinQuotes) {
            advanceEndQuotes();
        }
        else {
            advanceEndNoQuotes();
        }


        contentEnd = index;

        if(withinQuotes) {
            index += level.getFieldQuotes();
        }
        
        if(getLevel().isDelimiter(buffer.charAt(index))) {
            //advance past comma to position next read on either the
            //beginning of the field boundary or at the udt end
            ++index;
        }
    }
    
    public String getField() {
        findBoundaries();
        if(contentBegin == -1) {
            return null;
        }
        
        CharSequence seq = buffer.subSequence(contentBegin, contentEnd);
        if(anyQuotes(seq)) {
            return getLevel().replace(seq.toString());
        }
        else {
            return seq.toString();
        }
    }

    public void beginUdt() {
        while(isQuote(getCurrent())) {
            ++index;
        }
                
        final T next = factory.apply(buffer.charAt(index++), levels.size());
        levels.push(next);
    }

    public void endUdt() {
        final T last = levels.pop();
        ++index;
        index += last.getUdtQuotes();

        if(index < buffer.length() && last.isDelimiter(buffer.charAt(index))) {
            ++index;
        }
    }

    public char getCurrent() {
        return buffer.charAt(index);
    }

    public boolean hasMore() {
        return index < buffer.length();
    }
}
