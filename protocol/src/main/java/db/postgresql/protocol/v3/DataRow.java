package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.serializers.*;
import db.postgresql.protocol.v3.typeinfo.Registry;
import db.postgresql.protocol.v3.typeinfo.PgType;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class DataRow extends Response {

    private Session session;
    private final int numberColumns;
    private final int size;
    private boolean valid = true;
    private RowDescription rowDescription;
    
    public DataRow(final Session session, final int size) {
        super(BackEnd.DataRow);
        this.session = session;
        this.size = size;
        this.numberColumns = session.getShort() & 0xFFFF;
    }

    public DataRow setRowDescription(final RowDescription val) {
        this.rowDescription = val;
        return this;
    }

    public static final ResponseBuilder builder = (BackEnd backEnd, int size, Session session) -> {
        return new DataRow(session, size); };

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
        session.advance(size);
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
            final PgType pgType = Registry.pgType(session, field.typeOid);
            final Serializer serializer = Registry.serializer(session.getDatabase(), field.typeOid);
            if(field.typeOid == pgType.getOid()) {
                return serializer.read(session, session.getInt());
            }
            else {
                return serializer.readArray(session, session.getInt(), pgType.getDelimiter());
            }
        }

        public <T> T next(Class<T> type) {
            final Serializer<T> serializer = Registry.serializer(session.getDatabase(), type);
            return type.cast(serializer.read(session, session.getInt()));
        }
        
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public boolean nextBoolean() {
            return BooleanSerializer.instance.read(session, session.getInt());
        }
        
        public double nextDouble() {
            return DoubleSerializer.instance.read(session, session.getInt());
        }

        public float nextFloat() {
            return FloatSerializer.instance.read(session, session.getInt());
        }

        public int nextInt() {
            return IntSerializer.instance.read(session, session.getInt());
        }

        public long nextLong() {
            return LongSerializer.instance.read(session, session.getInt());
        }

        public short nextShort() {
            return ShortSerializer.instance.read(session, session.getInt());
        }
    }
}

