package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.serializers.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.temporal.TemporalAccessor;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.Function;

public class DataRow extends Response {

    private PostgresqlStream stream;
    private final int length;
    private boolean valid = true;
    private RowDescription rowDescription;
    
    public DataRow(final PostgresqlStream stream) {
        super(BackEnd.DataRow);
        this.stream = stream;
        this.length = stream.getShort() & 0xFFFF;
    }

    public DataRow setRowDescription(final RowDescription val) {
        this.rowDescription = val;
        return this;
    }

    public static final ResponseBuilder builder = (BackEnd backEnd, int size, PostgresqlStream stream) -> {
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
            final Serializer serializer = stream.serializer(field.typeOid);
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
            return stream.getMoneySerializer().read(stream, size, field.format);
        }

        public BigDecimal nextNumeric() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            return stream.getNumericSerializer().read(stream, size, field.format);
        }

        public boolean nextBoolean() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            return BooleanSerializer.instance.read(stream, size, field.format);
        }
        
        public BitSet nextBitSet() {
            guardAdvance();
            return BitSetSerializer.instance.read(stream, stream.getInt(), rowDescription.field(index++).format);
        }

        public byte[] nextBytes() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            return BytesSerializer.instance.read(stream, size, field.format);
        }

        public LocalDate nextDate() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            return DateSerializer.instance.read(stream, size, field.format);
        }

        public double nextDouble() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            return DoubleSerializer.instance.read(stream, size, field.format);
        }

        public float nextFloat() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            return FloatSerializer.instance.read(stream, size, field.format);
        }

        public int nextInt() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            return IntSerializer.instance.read(stream, size, field.format);
        }

        public long nextLong() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            return LongSerializer.instance.read(stream, size, field.format);
        }

        public short nextShort() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            return ShortSerializer.instance.read(stream, size, field.format);
        }

        public String nextString() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            return stream.getStringSerializer().read(stream, size, field.format);
        }

        public LocalTime nextLocalTime() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            return LocalTimeSerializer.instance.read(stream, size, field.format);
        }

        public OffsetTime nextOffsetTime() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            return OffsetTimeSerializer.instance.read(stream, size, field.format);
        }

        public LocalDateTime nextLocalDateTime() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            return LocalDateTimeSerializer.instance.read(stream, size, field.format);
        }

        public OffsetDateTime nextOffsetDateTime() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            final int size = stream.getInt();
            return OffsetDateTimeSerializer.instance.read(stream, size, field.format);
        }

        public UUID nextUUID() {
            guardAdvance();
            return UUIDSerializer.instance.read(stream, stream.getInt(), rowDescription.field(index++).format);
        }
    }
}

