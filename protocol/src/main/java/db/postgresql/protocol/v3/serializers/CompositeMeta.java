package db.postgresql.protocol.v3.serializers;

import java.util.function.BiFunction;

public abstract class CompositeMeta {

    public static final char[] BEGIN = { '(', '<', '{', '[' };
    public static final char[] END = { ')', '>', '}', ']' };
    public static final char STANDARD_DELIMITER = ',';
    public static final char QUOTE = '"';
    public static final String QUOTE_STR = "\"";

    private static boolean in(final char c, final char[] ary) {
        for(char e : ary) {
            if(c == e) {
                return true;
            }
        }

        return false;
    }
    
    public static boolean isBegin(final char c) {
        return in(c, BEGIN);
    }
    
    public static boolean isEnd(final char c) {
        return in(c, END);
    }

    public static boolean isQuote(final char c) {
        return c == QUOTE;
    }

    public abstract int getUdtQuotes();
    public abstract int getFieldQuotes();

    public int getEmbeddedQuotes() {
        return 2 * getFieldQuotes();
    }

    public boolean isEmbeddedQuotes(final int index, final CharSequence buffer) {
        final int len = getEmbeddedQuotes();
        
        if(len == 0) {
            return false;
        }

        if((buffer.length() - index) < len) {
            return false;
        }

        for(int i = 0; i < len; ++i) {
            if(!isQuote(buffer.charAt(i + index))) {
                return false;
            }
        }

        return true;
    }

    public boolean isDelimiter(final char c) {
        return c == delimiter;
    }
    
    public abstract String replace(final String str);

    private final char delimiter;
    private final char begin;
    private final int level;

    public char getDelimiter() {
        return delimiter;
    }

    public char getBegin() {
        return begin;
    }

    public char getEnd() {
        return END[beginIndex()];
    }
    
    public int getLevel() {
        return level;
    }

    private boolean validBegin() {
        return beginIndex() != -1;
    }
    
    public boolean validEnd(final char end) {
        final int idx = beginIndex();
        if(idx == -1) {
            return false;
        }
        else {
            return END[idx] == end;
        }
    }
    
    private int beginIndex() {
        for(int i = 0; i < BEGIN.length; ++i) {
            if(BEGIN[i] == begin) {
                return i;
            }
        }
        
        return -1;
    }

    protected CompositeMeta(final char begin, final int level) {
        this(begin, level, STANDARD_DELIMITER);
    }

    protected CompositeMeta(final char begin, final int level, final char delimiter) {
        this.begin = begin;
        this.level = level;
        this.delimiter = delimiter;
        assert(validBegin());
    }

    private static class UdtMeta extends CompositeMeta {
        
        public UdtMeta(final char begin, final int level) {
            super(begin, level);
        }

        public int getUdtQuotes() {
            return getLevel();
        }
        
        public int getFieldQuotes() {
            return getLevel() + 1;
        }

        public String replace(final String str) {
            final int total = getEmbeddedQuotes();
            StringBuilder sb = new StringBuilder(total);
            for(int i = 0; i < total; ++i) {
                sb.append(QUOTE);
            }

            return str.replace(sb.toString(), QUOTE_STR);
        }
    }

    private static class GeometryMeta extends CompositeMeta {

        public GeometryMeta(final char begin, final int level) {
            super(begin, level);
        }

        public int getUdtQuotes() {
            return 0;
        }

        public int getFieldQuotes() {
            return 0;
        }
        
        public String replace(final String str) {
            return str;
        }
    }

    private static class ArrayMeta extends CompositeMeta {
        public ArrayMeta(final char delimiter, final int level) {
            super('{', level, delimiter);
        }

        public int getUdtQuotes() {
            return 0;
        }

        public int getFieldQuotes() {
            return 1;
        }

        public String replace(final String str) {
            return str;
        }
    }

    public static BiFunction<Character,Integer,CompositeMeta> udt = (final Character begin, final Integer level) -> {
        return new UdtMeta(begin, level);
    };

    public static BiFunction<Character,Integer,CompositeMeta> geometry = (final Character begin, final Integer level) -> {
        return new GeometryMeta(begin, level);
    };

    public static BiFunction<Character,Integer,CompositeMeta> array = (final Character delimiter, final Integer level) -> {
        return new ArrayMeta(delimiter, level);
    };
}
