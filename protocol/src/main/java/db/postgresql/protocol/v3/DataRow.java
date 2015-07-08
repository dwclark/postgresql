package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.serializers.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class DataRow extends Response {

    private Stream stream;
    private final int length;
    private boolean valid = true;
    public final Map<Integer,Serializer> serializers;
    
    private DataRow(final Stream stream) {
        this(stream, ((PostgresqlStream) stream).getStandardSerializers());
    }

    public DataRow(final Stream stream, final Map<Integer,Serializer> serializers) {
        super(BackEnd.DataRow);
        this.serializers = serializers;
        this.stream = stream;
        this.length = stream.getShort() & 0xFFFF;
    }

    public static final ResponseBuilder builder = (BackEnd backEnd, int size, Stream stream) -> {
        return new DataRow(stream); };

    private void validate() {
        if(!valid) {
            throw new ProtocolException("You have already processed this DataRow");
        }
        
        valid = false;
    }
    
    public Iterator iterator(final RowDescription rowDescription) {
        validate();
        return new Iterator(rowDescription);
    }

    public void skip() {
        validate();
        for(int i = 0; i < length; ++i) {
            final int size = stream.getInt();
            stream.advance(size);
        }
    }

    public Object[] toArray(final RowDescription rowDescription) {
        Object [] ret = new Object[length];
        int index = 0;
        Iterator iter = iterator(rowDescription);
        while(iter.hasNext()) {
            ret[index++] = iter.next();
        }

        return ret;
    }
    
    public class Iterator implements java.util.Iterator<Object> {

        private final RowDescription rowDescription;
        private int index = 0;

        private Iterator(final RowDescription rowDescription) {
            this.rowDescription = rowDescription;
        }
        
        public boolean hasNext() {
            return index < length;
        }

        public void guardAdvance() {
            if(index == length) {
                throw new NoSuchElementException();
            }
        }
        
        public Object next() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final Serializer serializer = serializers.get(field.typeOid);
            final int size = stream.getInt();
            return serializer.readObject(stream, size, field.format);
        }
        
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public BigDecimal nextNumeric() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final MoneySerializer s = (MoneySerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public boolean nextBoolean() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final BooleanSerializer s = (BooleanSerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public byte[] nextBytes() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final BytesSerializer s = (BytesSerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public LocalDate nextDate() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final DateSerializer s = (DateSerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public double nextDouble() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final DoubleSerializer s = (DoubleSerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public float nextFloat() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final FloatSerializer s = (FloatSerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public int nextInt() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final IntSerializer s = (IntSerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public long nextLong() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final LongSerializer s = (LongSerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public short nextShort() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final ShortSerializer s = (ShortSerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public String nextString() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final StringSerializer s = (StringSerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public TemporalAccessor _nextTime() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final TimeSerializer s = (TimeSerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public LocalTime nextLocalTime() {
            return (LocalTime) _nextTime();
        }

        public OffsetTime nextOffsetTime() {
            return (OffsetTime) _nextTime();
        }

        public TemporalAccessor _nextDateTime() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final DateTimeSerializer s = (DateTimeSerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public LocalDateTime nextLocalDateTime() {
            return (LocalDateTime) _nextDateTime();
        }

        public OffsetDateTime nextOffsetDateTime() {
            return (OffsetDateTime) _nextDateTime();
        }
    }
}

