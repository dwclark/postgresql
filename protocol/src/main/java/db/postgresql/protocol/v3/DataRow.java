package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;

public class DataRow extends Response {

    private static class Extent {
        public Extent() { }
        
        public Extent(final Extent e) {
            this.size = e.size;
            this.position = e.position;
        }

        public Extent copy() {
            return new Extent(this);
        }
        
        int size;
        int position;
    }

    private static final int DEFAULT_EXTENTS = 64;
    protected Extent[] extents;
    protected int numberExtents;
    private RowDescription rowDescription;
    
    private DataRow() {
        super(BackEnd.DataRow);
        this.numberExtents = DEFAULT_EXTENTS;
        this.extents = copyAndAllocate(new Extent[0], DEFAULT_EXTENTS);
    }

    private DataRow(final DataRow toCopy) {
        super(toCopy);
        this.numberExtents = toCopy.numberExtents;
        this.extents = copyAndAllocate(toCopy.extents, numberExtents);
    }

    public RowDescription getRowDescription() {
        return rowDescription;
    }

    public void setRowDescription(final RowDescription val) {
        this.rowDescription = val;
    }

    @Override
    public DataRow copy() {
        return new DataRow(this);
    }

    private static Extent[] copyAndAllocate(final Extent[] toCopy, final int number) {
        Extent[] ret = new Extent[number];
        int index;
        for(index = 0; ((index < number) && (index < toCopy.length)); ++index) {
            ret[index] = toCopy[index].copy();
        }

        for(; index < number; ++index) {
            ret[index] = new Extent();
        }

        return ret;
    }

    public void ensurePositions(int total) {
        numberExtents = total;
        if(numberExtents < total) {
            extents = copyAndAllocate(extents, total);
        }
    }

    private void findPositions() {
        for(int i = 0; i < numberExtents; ++i) {
            extents[i].size = buffer.getInt();
            extents[i].position = buffer.position();
            if(extents[i].size > 0) {
                buffer.position(buffer.position() + extents[i].size);
            }
        }

        buffer.position(0);
    }

    private int checkIndex(int pos) {
        if(pos < numberExtents) {
            return pos;
        }
        else {
            throw new ArrayIndexOutOfBoundsException();
        }
    }

    public boolean isNull(int i) {
        return extents[checkIndex(i)].size == -1;
    }

    public int readInt(int pos) {
        Extent extent = extents[checkIndex(pos)];
        final int startAt = buffer.arrayOffset() + extent.position;
        return Integer.valueOf(new String(buffer.array(), startAt, extent.size, encoding));
    }

    public String readString(int pos) {
        Extent extent = extents[checkIndex(pos)];
        final int startAt = buffer.arrayOffset() + extent.position;
        return new String(buffer.array(), startAt, extent.size, encoding);
    }

    public static final ThreadLocal<DataRow> tlData = new ThreadLocal<DataRow>() {
            @Override protected DataRow initialValue() {
                return new DataRow();
            }
        };

    public static final ResponseBuilder builder = new ResponseBuilder() {

            public DataRow build(BackEnd backEnd, int size, Stream stream) {
                DataRow dr = tlData.get();
                dr.ensurePositions(stream.getShort() & 0xFFFF);
                dr.reset(stream.getRecord(size - 2), stream.getEncoding());
                dr.findPositions();
                return dr;
            }
        };
}

