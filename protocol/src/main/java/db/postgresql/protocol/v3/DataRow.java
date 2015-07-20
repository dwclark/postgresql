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
import java.util.function.Function;

public class DataRow extends Response {

    private Stream stream;
    private final int length;
    private boolean valid = true;
    public final Map<Integer,Serializer> serializers;
    private RowDescription rowDescription;
    
    private DataRow(final Stream stream) {
        this(stream, ((PostgresqlStream) stream).getStandardSerializers());
    }

    public DataRow(final Stream stream, final Map<Integer,Serializer> serializers) {
        super(BackEnd.DataRow);
        this.serializers = serializers;
        this.stream = stream;
        this.length = stream.getShort() & 0xFFFF;
    }

    public DataRow setRowDescription(final RowDescription val) {
        this.rowDescription = val;
        return this;
    }

    public static final ResponseBuilder builder = (BackEnd backEnd, int size, Stream stream) -> {
        return new DataRow(stream); };

    private void validate() {
        if(!valid) {
            throw new ProtocolException("You have already processed this DataRow");
        }
        
        valid = false;
    }
    
    public Iterator iterator() {
        validate();
        return new Iterator();
    }

    public void skip() {
        validate();
        for(int i = 0; i < length; ++i) {
            final int size = stream.getInt();
            stream.advance(size);
        }
    }

    public Object[] toArray() {
        return toObject((Iterator iter) -> {
                Object [] ret = new Object[length];
                int index = 0;
                while(iter.hasNext()) {
                    ret[index++] = iter.next();
                }

                return ret; });
    }

    public <R> R toObject(Function<Iterator,R> func) {
        return func.apply(iterator());
    }
    
    public class Iterator implements java.util.Iterator<Object> {

        private int index = 0;

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

        public Money nextMoney() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final MoneySerializer s = (MoneySerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public BigDecimal nextNumeric() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final NumericSerializer s = (NumericSerializer) serializers.get(field.typeOid);
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

        public LocalTime nextLocalTime() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final LocalTimeSerializer s = (LocalTimeSerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public OffsetTime nextOffsetTime() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final OffsetTimeSerializer s = (OffsetTimeSerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public LocalDateTime nextLocalDateTime() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final LocalDateTimeSerializer s = (LocalDateTimeSerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }

        public OffsetDateTime nextOffsetDateTime() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            final OffsetDateTimeSerializer s = (OffsetDateTimeSerializer) serializers.get(field.typeOid);
            return s.read(stream, size, field.format);
        }
    }
}

