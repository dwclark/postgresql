package db.postgresql.protocol.v3.serializers;

import java.util.function.BiFunction;

public abstract class CompositeMeta {

    public static final char STANDARD_DELIMITER = ',';
    public static final char QUOTE = '"';
    public static final String QUOTE_STR = "\"";
    
    public static boolean isQuote(final char c) {
        return c == QUOTE;
    }

    public abstract char getDelimiter();
    public abstract char getBegin();
    public abstract char getEnd();
    public abstract int getUdtQuotes();
    public abstract int getFieldQuotes();
    
    public boolean isEnd(final char c) {
        return getEnd() == c;
    }
    
    public boolean isBegin(final char c) {
        return getBegin() == c;
    }

    public boolean isDelimiter(final char c) {
        return getDelimiter() == c;
    }
    
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
    
    public String replace(final String str) {
        final int total = getEmbeddedQuotes();
        StringBuilder sb = new StringBuilder(total);
        for(int i = 0; i < total; ++i) {
            sb.append(QUOTE);
        }
        
        return str.replace(sb.toString(), QUOTE_STR);
    }

    private final int level;

    public int getLevel() {
        return level;
    }
    
    protected CompositeMeta(final int level) {
        this.level = level;
    }

    private static class UdtMeta extends CompositeMeta {
        private static final char BEGIN = '(';
        private static final char END = ')';

        public UdtMeta(final int level) {
            super(level);
        }

        public char getDelimiter() {
            return STANDARD_DELIMITER;
        }

        public char getBegin() {
            return BEGIN;
        }

        public char getEnd() {
            return END;
        }
        
        public int getUdtQuotes() {
            return getLevel();
        }
        
        public int getFieldQuotes() {
            return getLevel() + 1;
        }
    }

    private static class GeometryMeta extends CompositeMeta {
        public static final char[] BEGIN = { '(', '<', '{', '[' };
        public static final char[] END = { ')', '>', '}', ']' };

        private static boolean in(final char c, final char[] ary) {
            for(char e : ary) {
                if(c == e) {
                    return true;
                }
            }
            
            return false;
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

        private boolean validBegin() {
            return beginIndex() != -1;
        }

        final char begin;
        final char end;
        
        public GeometryMeta(final char begin, final int level) {
            super(level);
            this.begin = begin;
            this.end = END[beginIndex()];
            assert(validBegin());
        }

        public char getDelimiter() {
            return STANDARD_DELIMITER;
        }
        
        public char getBegin() {
            return begin;
        }
        
        public char getEnd() {
            return end;
        }
        
        public int getUdtQuotes() {
            return 0;
        }

        public int getFieldQuotes() {
            return 0;
        }

        @Override
        public String replace(final String str) {
            return str;
        }
    }

    private static class ArrayMeta extends CompositeMeta {
        private static final char BEGIN = '{';
        private static final char END = '}';

        private final char delimiter;
        
        public ArrayMeta(final int level, final char delimiter) {
            super(level);
            this.delimiter = delimiter;
        }
        
        public char getDelimiter() {
            return delimiter;
        }
        
        public char getBegin() {
            return BEGIN;
        }
        
        public char getEnd() {
            return END;
        }
        
        public int getUdtQuotes() {
            return 0;
        }

        public int getFieldQuotes() {
            return 1;
        }
    }

    public static BiFunction<Character,Integer,CompositeMeta> udt = (final Character begin, final Integer level) -> {
        return new UdtMeta(level);
    };

    public static BiFunction<Character,Integer,CompositeMeta> geometry = (final Character begin, final Integer level) -> {
        return new GeometryMeta(begin, level);
    };

    public static BiFunction<Character,Integer,CompositeMeta> array(final Character delimiter) {
        return (final Character begin, final Integer level) -> { return new ArrayMeta(level, delimiter); };
    }
}
