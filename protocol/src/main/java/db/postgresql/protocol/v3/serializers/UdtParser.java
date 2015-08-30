package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.ProtocolException;
import db.postgresql.protocol.v3.io.Stream;
import java.util.Stack;
import java.nio.CharBuffer;
import java.math.BigInteger;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class UdtParser implements UdtInput {

    final String buffer;
    final private Stack<Character> objectStack = new Stack<>();

    public static final char[] BEGIN = { '(', '<', '{', '[' };
    public static final char[] END = { ')', '>', '}', ']' };
    public static final char FIELD_DIV = ',';
    public static final char STR_DIV = '"';

    private int index = 0;
    private int beginField = 0;
    private int endField = 0;
    private boolean withinQuotes = false;

    public boolean hasNext() {
        return (index + 1) != buffer.length();
    }

    public char getCurrentDelimiter() {
        return objectStack.peek();
    }
    
    private static boolean valid(final char test, final char[] allowed) {
        for(char b : allowed) {
            if(b == test) {
                return true;
            }
        }

        return false;
    }
    
    private static boolean validBegin(final char test) {
        return valid(test, BEGIN);
    }

    private static boolean validEnd(final char test) {
        return valid(test, END);
    }

    private static boolean validFieldTermination(final char test) {
        return (test == FIELD_DIV) || validEnd(test);
    }

    private static boolean validDelimiters(final char begin, final char end) {
        int beginIndex = 0, endIndex = 0;
        for(; beginIndex < BEGIN.length; ++beginIndex) {
            if(BEGIN[beginIndex] == begin) {
                break;
            }
        }

        for(; endIndex < END.length; ++endIndex) {
            if(END[endIndex] == end) {
                break;
            }
        }

        return beginIndex == endIndex;
    }
    
    public UdtParser(final String buffer) {
        this.buffer = buffer;
    }

    public void beginUdt() {
        char b = buffer.charAt(index++);
        assert(validBegin(b));
        objectStack.push(b);
    }

    public void endUdt() {
        char end = buffer.charAt(index++);
        assert(validEnd(end));
        char begin = objectStack.pop();
        assert(validDelimiters(begin, end));

        if(index < buffer.length() && buffer.charAt(index) == FIELD_DIV) {
            ++index;
        }
    }

    public void findLimits() {
        beginField = index;
        while(true) {
            char next = buffer.charAt(index++);
            if(next == STR_DIV) {
                withinQuotes = withinQuotes ? false : true;
            }

            if(validFieldTermination(next) && !withinQuotes) {
                endField = index - 1;
                if(validEnd(next)) {
                    --index;
                }
                
                break;
            }
        }
    }

    public int limitSize() {
        return (endField - beginField);
    }
    
    public boolean readBoolean() {
        findLimits();
        if(limitSize() != 1) {
            throw new IllegalStateException("Field is too large to be a boolean");
        }
        
        char val = buffer.charAt(beginField);
        if(val == BooleanSerializer.T) {
            return true;
        }
        else if(val == BooleanSerializer.F) {
            return false;
        }
        else {
            throw new IllegalStateException(val + " is not a valid boolean value");
        }
    }

    //TODO: Make these more performant, no string allocation
    public short readShort() {
        findLimits();
        return Short.valueOf(buffer.subSequence(beginField, endField).toString());
    }

    public int readInt() {
        findLimits();
        return Integer.valueOf(buffer.subSequence(beginField, endField).toString());
    }

    public long readLong() {
        findLimits();
        return Long.valueOf(buffer.subSequence(beginField, endField).toString());
    }

    public float readFloat() {
        findLimits();
        return Float.valueOf(buffer.subSequence(beginField, endField).toString());
    }

    public double readDouble() {
        findLimits();
        String str = buffer.subSequence(beginField, endField).toString();
        return Double.valueOf(str);
    }

    public String readString() {
        findLimits();
        return buffer.substring(beginField, endField);
    }

    private static final Class[] CONSTRUCTOR_ARGS = new Class[] { UdtInput.class };
    
    public <T extends Udt> T readUdt(Class<T> type) {
        try {
            beginUdt();
            Constructor<T> constructor = type.getConstructor(CONSTRUCTOR_ARGS);
            T udt = (T) constructor.newInstance(this);
            endUdt();
            return udt;
        }
        catch(NoSuchMethodException | InstantiationException |
              IllegalAccessException | InvocationTargetException ex) {
            throw new ProtocolException(ex);
        }
    }
}
