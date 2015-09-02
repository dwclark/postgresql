package db.postgresql.protocol.v3.serializers;

import java.util.function.BiFunction;

public abstract class ParserMeta {

    public static final char[] BEGIN = { '(', '<', '{', '[' };
    public static final char[] END = { ')', '>', '}', ']' };
    public static final char DIV = ',';
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

    public static boolean isDiv(final char c) {
        return c == DIV;
    }

    public static boolean isQuote(final char c) {
        return c == QUOTE;
    }

    public abstract int getUdtQuotes();
    public abstract int getFieldQuotes();
    public abstract int getEmbeddedQuotes();

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
    
    public abstract String replace(final String str);

    private final char delimiter;
    private final int level;

    public char getDelimiter() {
        return delimiter;
    }

    public char getEndDelimiter() {
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
            if(BEGIN[i] == delimiter) {
                return i;
            }
        }
        
        return -1;
    }

    protected ParserMeta(final char delimiter, final int level) {
        this.delimiter = delimiter;
        this.level = level;
        assert(validBegin());
    }

    private static class UdtMeta extends ParserMeta {
        
        public UdtMeta(final char delimiter, final int level) {
            super(delimiter, level);
        }

        public int getUdtQuotes() {
            return getLevel();
        }
        
        public int getFieldQuotes() {
            return getLevel() + 1;
        }

        public int getEmbeddedQuotes() {
            return getFieldQuotes() * 2;
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

    private static class GeometryMeta extends ParserMeta {

        public GeometryMeta(final char delimiter, final int level) {
            super(delimiter, level);
        }

        public int getUdtQuotes() {
            return 0;
        }

        public int getFieldQuotes() {
            return 0;
        }
        
        public int getEmbeddedQuotes() {
            return 0;
        }
        
        public String replace(final String str) {
            return str;
        }
    }

    public static BiFunction<Character,Integer,ParserMeta> udt = (final Character delimiter, final Integer level) -> {
        return new UdtMeta(delimiter, level);
    };

    public static BiFunction<Character,Integer,ParserMeta> geometry = (final Character delimiter, final Integer level) -> {
        return new GeometryMeta(delimiter, level);
    };
}
