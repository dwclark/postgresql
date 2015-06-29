package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.nio.ByteBuffer;

public class DataRow extends Response {

    private static final int SIZE = 0;
    private static final int POS = 1;
    protected int[][] positions;
    protected int numberPositions;
    
    private DataRow() {
        super(BackEnd.DataRow);
        positions = new int[256][2];
    }

    private DataRow(DataRow toCopy) {
        super(toCopy);
        this.numberPositions = toCopy.numberPositions;
        this.positions = new int[numberPositions][2];
        for(int i = 0; i < numberPositions; ++i) {
            positions[i][SIZE] = toCopy.positions[i][SIZE];
            positions[i][POS] = toCopy.positions[i][POS];
        }
    }

    public void ensurePositions(int total) {
        numberPositions = total;
        if(positions.length < total) {
            positions = new int[numberPositions][2];
        }
    }

    private void findPositions() {
        for(int i = 0; i < numberPositions; ++i) {
            positions[i][SIZE] = buffer.getInt();
            positions[i][POS] = buffer.position();
            if(positions[i][SIZE] > 0) {
                buffer.position(buffer.position() + positions[i][SIZE]);
            }
        }

        buffer.position(0);
    }

    private int checkIndex(int pos) {
        if(pos < numberPositions) {
            return pos;
        }
        else {
            throw new ArrayIndexOutOfBoundsException();
        }
    }

    public boolean isNull(int i) {
        return positions[checkIndex(i)][SIZE] == -1;
    }

    public int extractInt(RowDescription rd, int pos) {
        int[] extent = positions[checkIndex(pos)];
        final int startAt = buffer.arrayOffset() + extent[POS];
        return Integer.valueOf(new String(buffer.array(), startAt, extent[SIZE], encoding));
    }

    public String extractString(RowDescription rd, int pos) {
        int[] extent = positions[checkIndex(pos)];
        final int startAt = buffer.arrayOffset() + extent[POS];
        return new String(buffer.array(), startAt, extent[SIZE], encoding);
    }

    @Override
    public DataRow copy() {
        return new DataRow(this);
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

