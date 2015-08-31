package db.postgresql.protocol.v3.serializers;

import java.util.function.BiFunction;

public abstract class ParserMeta {

    public static final char[] BEGIN = { '(', '<', '{', '[' };
    public static final char[] END = { ')', '>', '}', ']' };
    public static final char DIV = ',';
    public static final char QUOTE = '"';
    public static final String QUOTE_STR = "\"";
    
    public abstract int getFieldQuotes();
    public abstract int getEmbeddedQuotes();
    public abstract String replace(final String str);

    private final char delimiter;
    private final int level;

    public char getDelimiter() {
        return delimiter;
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
