package db.postgresql.protocol.v3.serializers;

import java.util.Calendar;
import java.util.TimeZone;
import java.math.BigDecimal;
import java.util.List;
import java.util.Arrays;
import java.nio.ByteBuffer;
import db.postgresql.protocol.v3.Extent;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.ProtocolException;
import java.sql.Time;
import java.sql.Date;
import java.sql.Timestamp;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.regex.MatchResult;

public abstract class Serializer {

    private final int[] oids;

    public int[] getOids() {
        return oids;
    }

    private final Class[] types;
    
    public Class[] getTypes() {
        return types;
    }

    public Serializer(final int[] oids, final Class... types) {
        this.oids = oids;
        this.types = types;
    }

    //handle primitives separately to avoid object creation overhead
    public boolean readBoolean(final ByteBuffer buffer, final Extent extent, final Format format) {
        throw new UnsupportedOperationException();
    }
    
    public short readShort(final ByteBuffer buffer, final Extent extent, final Format format) {
        throw new UnsupportedOperationException();
    }

    public int readInt(final ByteBuffer buffer, final Extent extent, final Format format) {
        throw new UnsupportedOperationException();
    }

    public long readLong(final ByteBuffer buffer, final Extent extent, final Format format) {
        throw new UnsupportedOperationException();
    }

    public float readFloat(final ByteBuffer buffer, final Extent extent, final Format format) {
        throw new UnsupportedOperationException();
    }

    public double readDouble(final ByteBuffer buffer, final Extent extent, final Format format) {
        throw new UnsupportedOperationException();
    }
    
    public BigDecimal readBigDecimal(final ByteBuffer buffer, final Extent extent, final Format format) {
        throw new UnsupportedOperationException();
    }

    public byte[] readBytes(final ByteBuffer buffer, final Extent extent, final Format format) {
        throw new UnsupportedOperationException();
    }

    public Date readDate(final ByteBuffer buffer, final Extent extent, final Format format) {
        throw new UnsupportedOperationException();
    }

    public Time readTime(final ByteBuffer buffer, final Extent extent, final Format format) {
        throw new UnsupportedOperationException();
    }

    public Timestamp readTimestamp(final ByteBuffer buffer, final Extent extent, final Format format) {
        throw new UnsupportedOperationException();
    }

    public String readString(final ByteBuffer buffer, final Extent extent,
                             final Format format, final Charset encoding) {
        throw new UnsupportedOperationException();
    }

    public Object readObject(final ByteBuffer buffer, final Extent extent,
                             final Format format, final Charset encoding) {
        throw new UnsupportedOperationException();
    }

    private static int[] oids(int... vals) {
        return vals;
    }

    private static Class[] classes(Class... vals) {
        return vals;
    }

    private static final byte SIGN = (byte) '-';
    private static final byte T = (byte) 't';
    private static final byte F = (byte) 'f';
    private static final String INFINITY = "Infinity";
    private static final String NEG_INFINITY = "-Infinity";
    private static final String NAN = "NaN";

    private static final String ISO_TIME_STR = "(\\d\\d):(\\d\\d):(\\d\\d)\\.(\\d{6})(([\\-|\\+]\\d\\d)?)";
    private static final String ISO_DATE_STR = "(\\d{4})-(\\d{2})-(\\d{2})";
    private static final String ISO_TIMESTAMP_STR = ISO_DATE_STR + " " + ISO_TIME_STR;
    private static final Pattern ISO_TIME = Pattern.compile(ISO_TIME_STR);
    private static final Pattern ISO_DATE = Pattern.compile(ISO_DATE_STR);
    private static final Pattern ISO_TIMESTAMP = Pattern.compile(ISO_TIMESTAMP_STR);

    private static final Charset ASCII_ENCODING = Charset.forName("US-ASCII");
    private static String _str(final ByteBuffer buffer, final Extent extent, final Charset encoding) {
        final int startAt = buffer.arrayOffset() + extent.position;
        return new String(buffer.array(), startAt, extent.size, encoding);
    }

    public static class BooleanSerializer extends Serializer {

        public BooleanSerializer() {
            super(oids(16), classes(boolean.class, Boolean.class));
        }

        @Override
        public boolean readBoolean(final ByteBuffer buffer, final Extent extent, final Format format) {
            return buffer.get(extent.getPosition()) == T ? true : false;
        }

        @Override
        public Object readObject(final ByteBuffer buffer, final Extent extent,
                                 final Format format, final Charset encoding) {
            return readBoolean(buffer, extent, format);
        }
    }

    public static class ShortSerializer extends Serializer {
        public ShortSerializer() {
            super(oids(21), classes(short.class, Short.class));
        }

        @Override
        public short readShort(final ByteBuffer buffer, final Extent extent, final Format format) {
            if(extent.isNull()) {
                return 0;
            }
            
            short accum = 0;
            short multiplier = 1;
            for(int i = extent.getLast(); i >= extent.getPosition(); --i) {
                byte val = buffer.get(i);
                if(val == SIGN) {
                    return (short) -accum;
                }
                else {
                    accum += (Character.digit(val, 10) * multiplier);
                    multiplier *= 10;
                }
            }

            return accum;
        }

        @Override
        public Object readObject(final ByteBuffer buffer, final Extent extent,
                                 final Format format, final Charset encoding) {
            return extent.isNull() ? null : readShort(buffer, extent, format);
        }
    }

    public static class IntSerializer extends Serializer {
        public IntSerializer() {
            super(oids(21,23), classes(short.class, Short.class, int.class, Integer.class));
        }

        @Override
        public int readInt(final ByteBuffer buffer, final Extent extent, final Format format) {
            if(extent.isNull()) {
                return 0;
            }
            
            int accum = 0;
            int multiplier = 1;
            for(int i = extent.getLast(); i >= extent.getPosition(); --i) {
                byte val = buffer.get(i);
                if(val == SIGN) {
                    return -accum;
                }
                else {
                    accum += (Character.digit(val, 10) * multiplier);
                    multiplier *= 10;
                }
            }

            return accum;
        }

        @Override
        public Object readObject(final ByteBuffer buffer, final Extent extent,
                                 final Format format, final Charset encoding) {
            return extent.isNull() ? null : readInt(buffer, extent, format);
        }
    }

    public static class LongSerializer extends Serializer {
        public LongSerializer() {
            super(oids(20, 21,23), classes(short.class, Short.class, int.class, Integer.class,
                                           long.class, Long.class));
        }

        @Override
        public long readLong(final ByteBuffer buffer, final Extent extent, final Format format) {
            if(extent.isNull()) {
                return 0L;
            }
            
            long accum = 0;
            long multiplier = 1;
            for(int i = extent.getLast(); i >= extent.getPosition(); --i) {
                byte val = buffer.get(i);
                if(val == SIGN) {
                    return -accum;
                }
                else {
                    accum += (Character.digit(val, 10) * multiplier);
                    multiplier *= 10;
                }
            }

            return accum;
        }

        @Override
        public Object readObject(final ByteBuffer buffer, final Extent extent,
                                 final Format format, final Charset encoding) {
            return extent.isNull() ? null : readLong(buffer, extent, format);
        }
    }

    public static class FloatSerializer extends Serializer {
        public FloatSerializer() {
            super(oids(700), classes(float.class, Float.class));
        }

        @Override
        public float readFloat(final ByteBuffer buffer, final Extent extent, final Format format) {
            if(extent.isNull()) {
                return 0.0f;
            }

            String str = _str(buffer, extent, ASCII_ENCODING);
            switch(str) {
            case NAN: return Float.NaN;
            case INFINITY: return Float.POSITIVE_INFINITY;
            case NEG_INFINITY: return Float.NEGATIVE_INFINITY;
            default: return Float.valueOf(str);
            }
        }

        @Override
        public Object readObject(final ByteBuffer buffer, final Extent extent,
                                 final Format format, final Charset encoding) {
            return extent.isNull() ? null : readFloat(buffer, extent, format);
        }
    }

    public static class DoubleSerializer extends Serializer {
        public DoubleSerializer() {
            super(oids(700,701), classes(float.class, Float.class, double.class, Double.class));
        }

        @Override
        public double readDouble(final ByteBuffer buffer, final Extent extent, final Format format) {
            if(extent.isNull()) {
                return 0.0f;
            }

            String str = _str(buffer, extent, ASCII_ENCODING);
            switch(str) {
            case NAN: return Double.NaN;
            case INFINITY: return Double.POSITIVE_INFINITY;
            case NEG_INFINITY: return Double.NEGATIVE_INFINITY;
            default: return Double.valueOf(str);
            }
        }

        @Override
        public Object readObject(final ByteBuffer buffer, final Extent extent,
                                 final Format format, final Charset encoding) {
            return extent.isNull() ? null : readFloat(buffer, extent, format);
        }
    }

    public static class BigDecimalSerializer extends Serializer {
        public BigDecimalSerializer() {
            super(oids(790,1700), classes(BigDecimal.class));
        }

        @Override
        public BigDecimal readBigDecimal(final ByteBuffer buffer, final Extent extent, final Format format) {
            if(extent.isNull()) {
                return null;
            }

            String str = _str(buffer, extent, ASCII_ENCODING);
            return new BigDecimal(str);
        }

        @Override
        public Object readObject(final ByteBuffer buffer, final Extent extent, final Format format, final Charset encoding) {
            return readBigDecimal(buffer, extent, format);
        }
    }

    public static class DateSerializer extends Serializer {
        public DateSerializer() {
            super(oids(1082), classes(Date.class));
        }

        public static Date iso(final String str) {
            final Matcher m = ISO_DATE.matcher(str);
            if(!m.matches()) {
                throw new ProtocolException(str + " is not a valid iso date format");
            }

            final Calendar cal = Calendar.getInstance();
            final int years = Integer.valueOf(m.group(1));
            final int months = Integer.valueOf(m.group(2));
            final int days = Integer.valueOf(m.group(3));
            cal.set(years, months, days, 0, 0, 0);
            return new Date(cal.getTime().getTime());
        }

        @Override
        public Date readDate(final ByteBuffer buffer, final Extent extent, final Format format) {
            if(extent.isNull()) {
                return null;
            }

            return iso(_str(buffer, extent, ASCII_ENCODING));
        }

        @Override
        public Object readObject(final ByteBuffer buffer, final Extent extent,
                                 final Format format, final Charset encoding) {
            return readDate(buffer, extent, format);
        }
    }

    public static class TimeSerializer extends Serializer {

        public TimeSerializer() {
            super(oids(1083), classes(Time.class));
        }

        private static Calendar cal(final MatchResult result) {
            if(result.groupCount() == 4) {
                return Calendar.getInstance();
            }
            else {
                return Calendar.getInstance(TimeZone.getTimeZone("GMT" + result.group(5)));
            }
        }

        public static Time iso(final String str) {
            final Matcher m = ISO_TIME.matcher(str);
            if(!m.matches()) {
                throw new ProtocolException(str + " is not a recognized time format");
            }

            final MatchResult result = m.toMatchResult();
            final int hours = Integer.valueOf(m.group(1));
            final int minutes = Integer.valueOf(m.group(2));
            final int micros = Integer.valueOf(m.group(4));
            final int seconds = Integer.valueOf(m.group(3)) + ((micros >= 500_000) ? 1 : 0);

            final Calendar cal = cal(result);
            cal.set(0, 0, 0, hours, minutes, seconds);
            return new Time(cal.getTime().getTime());
        }
        
        @Override
        public Time readTime(final ByteBuffer buffer, final Extent extent, final Format format) {
            if(extent.isNull()) {
                return null;
            }

            return iso(_str(buffer, extent, ASCII_ENCODING));
        }

        @Override
        public Object readObject(final ByteBuffer buffer, final Extent extent,
                                 final Format format, final Charset encoding) {
            return readTime(buffer, extent, format);
        }
    }

    private static Calendar cal(final MatchResult result) {
        if(result.groupCount() == 7) {
            return Calendar.getInstance();
        }
        else {
            return Calendar.getInstance(TimeZone.getTimeZone("GMT" + result.group(8)));
        }
    }
    
    public static class TimestampSerializer extends Serializer {

        public TimestampSerializer() {
            super(oids(1114), classes(Timestamp.class));
        }

        public static Timestamp iso(final String str) {
            final Matcher m = ISO_TIMESTAMP.matcher(str);
            if(!m.matches()) {
                throw new ProtocolException(str + " is not a recognized time format");
            }

            final MatchResult result = m.toMatchResult();
            final int years = Integer.valueOf(m.group(1));
            final int months = Integer.valueOf(m.group(2));
            final int days = Integer.valueOf(m.group(3));
            final int hours = Integer.valueOf(m.group(4));
            final int minutes = Integer.valueOf(m.group(5));
            final int seconds = Integer.valueOf(m.group(6));
            final int nanos = Integer.valueOf(m.group(7)) * 1000;

            final Calendar cal = cal(result);
            cal.set(years, months, days, hours, minutes, seconds);
            final Timestamp tstamp = new Timestamp(cal.getTime().getTime());
            tstamp.setNanos(nanos);
            return tstamp;
        }
        
        @Override
        public Timestamp readTimestamp(final ByteBuffer buffer, final Extent extent, final Format format) {
            if(extent.isNull()) {
                return null;
            }

            return iso(_str(buffer, extent, ASCII_ENCODING));
        }

        @Override
        public Object readObject(final ByteBuffer buffer, final Extent extent,
                                 final Format format, final Charset encoding) {
            return readTimestamp(buffer, extent, format);
        }
    }

    public static class StringSerializer extends Serializer {

        public StringSerializer() {
            super(oids(25,1043), classes(String.class));
        }
        
        @Override
        public String readString(final ByteBuffer buffer, final Extent extent,
                                 final Format format, final Charset encoding) {
            if(extent.isNull()) {
                return null;
            }

            return _str(buffer, extent, encoding);
        }

        @Override
        public Object readObject(final ByteBuffer buffer, final Extent extent,
                                 final Format format, final Charset encoding) {
            return readString(buffer, extent, format, encoding);
        }
    }

    public static class BytesSerializer extends Serializer {

        public BytesSerializer() {
            super(oids(17), classes(byte.class, byte[].class));
        }
        
        @Override
        public byte[] readBytes(final ByteBuffer buffer, final Extent extent, final Format format) {
            if(extent.isNull()) {
                return null;
            }

            final int total = (extent.size - 2) / 2;
            final int base = extent.position + 2;
            byte[] ret = new byte[total];
            for(int i = 0; i < total; ++i) {
                final int shift = i * 2;
                int first = buffer.get(base + shift);
                int second = buffer.get(base + shift + 1);
                ret[i] = (byte) ((Character.digit(first, 16) * 16) + Character.digit(second, 16));
            }

            return ret;
        }

        @Override
        public Object readObject(final ByteBuffer buffer, final Extent extent,
                                 final Format format, final Charset encoding) {
            return readBytes(buffer, extent, format);
        }
    }
}
