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

        private FieldDescriptor field() {
            if(index == length) {
                throw new NoSuchElementException();
            }

            return rowDescription.field(index++);
        }
        
        public Object next() {
            final FieldDescriptor field = field();
            final Serializer serializer = stream.serializer(field.typeOid);
            return serializer.readObject(stream, stream.getInt(), field.format);
        }

        public <T> T next(Class<T> type) {
            final Serializer serializer = stream.serializer(type);
            return type.cast(serializer.readObject(stream, stream.getInt(), field().format));
        }
        
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public boolean nextBoolean() {
            return BooleanSerializer.instance.read(stream, stream.getInt(), field().format);
        }
        
        public double nextDouble() {
            return DoubleSerializer.instance.read(stream, stream.getInt(), field().format);
        }

        public float nextFloat() {
            return FloatSerializer.instance.read(stream, stream.getInt(), field().format);
        }

        public int nextInt() {
            return IntSerializer.instance.read(stream, stream.getInt(), field().format);
        }

        public long nextLong() {
            return LongSerializer.instance.read(stream, stream.getInt(), field().format);
        }

        public short nextShort() {
            return ShortSerializer.instance.read(stream, stream.getInt(), field().format);
        }
    }
}

