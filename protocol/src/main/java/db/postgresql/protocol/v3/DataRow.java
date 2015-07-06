package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.serializers.*;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.SQLData;
import java.util.NoSuchElementException;
import java.util.Map;
import java.util.Collections;
import java.util.HashMap;
import java.time.temporal.TemporalAccessor;

public class DataRow extends Response {

    private Stream stream;
    private final int length;
    private boolean valid = true;
    
    private DataRow(final Stream stream) {
        super(BackEnd.DataRow);
        this.stream = stream;
        this.length = stream.getShort() & 0xFFFF;
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public DataRow build(BackEnd backEnd, int size, Stream stream) {
                return new DataRow(stream);
            }
        };

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

    public static final Map<Integer,Serializer> standard;

    private static void put(Map<Integer,Serializer> map, Serializer serializer) {
        assert(serializer != null);
        assert(serializer.getOids() != null);
        for(int oid : serializer.getOids()) {
            map.put(oid, serializer);
        }
    }
    
    static {
        Map<Integer,Serializer> tmp = new HashMap<>();
        put(tmp, BigDecimalSerializer.instance);
        put(tmp, BooleanSerializer.instance);
        put(tmp, BytesSerializer.instance);
        put(tmp, DateSerializer.instance);
        put(tmp, DoubleSerializer.instance);
        put(tmp, FloatSerializer.instance);
        put(tmp, IntSerializer.instance);
        put(tmp, LongSerializer.instance);
        put(tmp, ShortSerializer.instance);
        put(tmp, StringSerializer.instance);
        put(tmp, TimeSerializer.instance);
        put(tmp, DateTimeSerializer.instance);
        standard = Collections.unmodifiableMap(tmp);
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
            final Serializer serializer = standard.get(field.typeOid);
            final int size = stream.getInt();
            return serializer.readObject(stream, size, field.format);
        }
        
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public BigDecimal nextBigDecimal() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            BigDecimalSerializer.instance.checkHandles(field.typeOid);
            final int size = stream.getInt();
            return BigDecimalSerializer.instance.read(stream, size, field.format);
        }

        public boolean nextBoolean() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            BooleanSerializer.instance.checkHandles(field.typeOid);
            final int size = stream.getInt();
            return BooleanSerializer.instance.read(stream, size, field.format);
        }

        public byte[] nextBytes() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            BytesSerializer.instance.checkHandles(field.typeOid);
            final int size = stream.getInt();
            return BytesSerializer.instance.read(stream, size, field.format);
        }

        public Date nextDate() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            DateSerializer.instance.checkHandles(field.typeOid);
            final int size = stream.getInt();
            return DateSerializer.instance.read(stream, size, field.format);
        }

        public double nextDouble() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            DoubleSerializer.instance.checkHandles(field.typeOid);
            final int size = stream.getInt();
            return DoubleSerializer.instance.read(stream, size, field.format);
        }

        public float nextFloat() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            FloatSerializer.instance.checkHandles(field.typeOid);
            final int size = stream.getInt();
            return FloatSerializer.instance.read(stream, size, field.format);
        }

        public int nextInt() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            IntSerializer.instance.checkHandles(field.typeOid);
            final int size = stream.getInt();
            return IntSerializer.instance.read(stream, size, field.format);
        }

        public long nextLong() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            LongSerializer.instance.checkHandles(field.typeOid);
            final int size = stream.getInt();
            return LongSerializer.instance.read(stream, size, field.format);
        }

        public short nextShort() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            ShortSerializer.instance.checkHandles(field.typeOid);
            final int size = stream.getInt();
            return ShortSerializer.instance.read(stream, size, field.format);
        }

        public String nextString() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            StringSerializer.instance.checkHandles(field.typeOid);
            final int size = stream.getInt();
            return StringSerializer.instance.read(stream, size, field.format);
        }

        public Time nextTime() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            TimeSerializer.instance.checkHandles(field.typeOid);
            final int size = stream.getInt();
            return TimeSerializer.instance.read(stream, size, field.format);
        }

        public TemporalAccessor nextDateTime() {
            guardAdvance();
            final FieldDescriptor field = rowDescription.field(index++);
            DateTimeSerializer.instance.checkHandles(field.typeOid);
            final int size = stream.getInt();
            return DateTimeSerializer.instance.read(stream, size, field.format);
        }

        public SQLData next(Class<? extends SQLData> type) {
            guardAdvance();
            throw new UnsupportedOperationException();
        }
    }
}

