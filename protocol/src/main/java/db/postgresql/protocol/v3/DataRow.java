package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.PostgresqlStream;
import db.postgresql.protocol.v3.serializers.*;
import db.postgresql.protocol.v3.typeinfo.Registry;
import db.postgresql.protocol.v3.typeinfo.PgType;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class DataRow extends Response {

    private final PostgresqlStream stream;
    private final int numberColumns;
    private boolean valid = true;
    private RowDescription rowDescription;
    
    public DataRow(final PostgresqlStream stream, final int size) {
        super(BackEnd.DataRow, size);
        this.stream = stream;
        this.numberColumns = stream.getShort() & 0xFFFF;
    }

    public DataRow setRowDescription(final RowDescription val) {
        this.rowDescription = val;
        return this;
    }

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
        stream.advance(size);
    }

    public Object[] toArray() {
        return toObject((Iterator iter) -> {
                Object [] ret = new Object[numberColumns];
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
            return index < numberColumns;
        }

        private FieldDescriptor field() {
            if(index == numberColumns) {
                throw new NoSuchElementException();
            }

            return rowDescription.field(index++);
        }
        
        public Object next() {
            final FieldDescriptor field = field();
            final PgType pgType = Registry.pgType(stream, field.typeOid);
            final Serializer serializer = Registry.serializer(stream, pgType);
            if(field.typeOid == pgType.getOid()) {
                return serializer.read(stream, stream.getInt());
            }
            else {
                return serializer.readArray(stream, stream.getInt(), pgType.getDelimiter());
            }
        }

        public <T> T next(Class<T> type) {
            final Serializer<T> serializer = Registry.serializer(stream.getDatabase(), type);
            return type.cast(serializer.read(stream, stream.getInt()));
        }
        
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public boolean nextBoolean() {
            return BooleanSerializer.instance.read(stream, stream.getInt());
        }
        
        public double nextDouble() {
            return DoubleSerializer.instance.read(stream, stream.getInt());
        }

        public float nextFloat() {
            return FloatSerializer.instance.read(stream, stream.getInt());
        }

        public int nextInt() {
            return IntSerializer.instance.read(stream, stream.getInt());
        }

        public long nextLong() {
            return LongSerializer.instance.read(stream, stream.getInt());
        }

        public short nextShort() {
            return ShortSerializer.instance.read(stream, stream.getInt());
        }
    }
}

