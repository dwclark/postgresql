package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.nio.ByteBuffer;

public class DataRow extends Response {

    protected int[] positions = new int[256];
    protected int numberPositions;
    
    private DataRow() {
        super(BackEnd.DataRow);
    }

    private DataRow(DataRow dr) {
        super(BackEnd.DataRow, dr);
        this.numberPositions = dr.numberPositions;
        this.positions = new int[numberPositions];
        System.arraycopy(dr.positions, 0, positions, 0, numberPositions);
    }

    public void ensurePositions(int total) {
        numberPositions = total;
        if(positions.length < total) {
            positions = new int[numberPositions];
        }

        return positions;
    }

    private void findPositions(ByteBuffer buffer) {
        this.buffer = buffer;
        for(int i = 0; i < numberPositions; ++i) {
            positions[i] = buffer.getInt();
            if(positions[i] > 0) {
                buffer.position(buffer.position() + position[i]);
            }
        }

        buffer.position(0);
    }

    public boolean isNull(int i) {
        if(i >= numPositions) {
            throw new ArrayIndexOutOfBoundsException();
        }
        
        return positions[i] == -1;
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

