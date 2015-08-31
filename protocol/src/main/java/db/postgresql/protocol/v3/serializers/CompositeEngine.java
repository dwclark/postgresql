package db.postgresql.protocol.v3.serializers;

import java.util.Deque;
import java.util.ArrayDeque;
import java.util.function.BiFunction;
import static db.postgresql.protocol.v3.serializers.ParserMeta.*;

public class CompositeEngine {

    private final Deque<ParserMeta> levels = new ArrayDeque<>();
    private final BiFunction<Character,Integer,ParserMeta> factory;
    
    final CharSequence buffer;
    private int _index;
    private int _fieldBegin;
    private int _fieldEnd;
    private int _contentBegin;
    private int _contentEnd;

    public int getFieldBegin() { return _fieldBegin; }
    public int getFieldEnd() { return _fieldEnd; }
    public int getContentBegin() { return _contentBegin; }
    public int getContentEnd() { return _contentEnd; }
    
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

    private static boolean endUdtChar(final char c) {
        for(char end : END) {
            if(c == end) {
                return true;
            }
        }
        
        return false;
    }

    private static boolean noQuoteEnd(final char c) {
        return (c == ',') ? true : endUdtChar(c);
    }
    
    private int contentEndNoQuotes(final int start) {
        int idx = start;
        while(!noQuoteEnd(buffer.charAt(idx))) {
            ++idx;
        }

        return idx;
    }

    private int contentEndWithQuotes(final int start) {
        final ParserMeta level = getLevel();
        int idx = start;
        while(true) {
            if(buffer.charAt(idx) != QUOTE) {
                ++idx;
                continue;
            }

            //it's a quote, figure out what to do
            final int remaining = buffer.length() - idx;
            
            if(remaining < level.getEmbeddedQuotes() || !allQuotes(buffer.subSequence(idx, idx + level.getEmbeddedQuotes()))) {
                return idx;
            }

            //it's all quotes, advance and continue
            idx += level.getEmbeddedQuotes();
        }
    }
    
    private void prepareField() {
        boolean withinQuotes = false;
        _fieldBegin = _index;

        //detect null content
        if(noQuoteEnd(buffer.charAt(_index))) {
            _fieldEnd = _index;
            _contentBegin = _index;
            _contentEnd = _index;
            return;
        }

        //advance to content
        if(buffer.charAt(_index) == QUOTE) {
            _index += getLevel().getFieldQuotes();
            withinQuotes = true;
        }

        _contentBegin = _index;
        _index = withinQuotes ? contentEndWithQuotes(_index) : contentEndNoQuotes(_index);
        _contentEnd = _index;

        if(withinQuotes) {
            _index += getLevel().getFieldQuotes();
        }

        _fieldEnd = _index;
    }

    public String getField() {
        prepareField();
        if(_fieldBegin == _fieldEnd) {
            return null;
        }

        CharSequence seq = buffer.subSequence(_contentBegin, _contentEnd);
        if(anyQuotes(seq)) {
            return getLevel().replace(seq.toString());
        }
        else {
            return seq.toString();
        }
    }

    public void beginUdt() {
        if(levels.size() > 0) {
            ++_index;
        }

        final char delimiter = buffer.charAt(_index);
        final ParserMeta next = factory.apply(delimiter, levels.size());
        _index += next.getLevel();
        levels.push(next);
    }

    public void endUdt() {
        final char delimiter = buffer.charAt(_index);
        final ParserMeta last = levels.pop();
        ++_index; //advance the delimiter
        _index += last.getLevel(); //advance any quotes
        assert(last.validEnd(delimiter));
    }

    public boolean hasNext() {
        return _index < buffer.length(); 
    }
}
