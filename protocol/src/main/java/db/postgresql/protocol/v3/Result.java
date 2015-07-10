package db.postgresql.protocol.v3;

import java.util.List;
import java.util.EnumSet;
import java.util.NoSuchElementException;

public class Result {

    private final Session stream;
    private final Response response;
    private TransactionStatus status;

    private static final EnumSet<BackEnd> WILL_HANDLE =
        EnumSet.of(BackEnd.RowDescription, BackEnd.EmptyQueryResponse);

    private Result(final Session stream) {
        this.stream = stream;
        this.response = stream.next(WILL_HANDLE);
        detectLast();
    }

    public boolean isLast() {
        if(status == null) {
            detectLast();
        }
        
        return status != null;
    }

    public boolean isDataResult() {
        return response instanceof RowDescription;
    }

    public DataRowIterator iterator() {
        assert(isDataResult());
        return new DataRowIterator((RowDescription) response);
    }

    public Result nextResult() {
        assert(!isLast());

        return new Result(stream);
    }

    protected void detectLast() {
        final BackEnd backEnd = BackEnd.find(stream.peek());
        if(backEnd == BackEnd.ReadyForQuery) {
            ReadyForQuery rfq = (ReadyForQuery) stream.next(EnumSet.of(BackEnd.ReadyForQuery));
            status = rfq.getStatus();
        }
    }
    
    public static Result execute(final List<String> queries, final Session stream) {
        for(String query : queries) {
            stream.query(query);
        }

        return new Result(stream);
    }

    private static final EnumSet<BackEnd> DRI_WILL_HANDLE =
        EnumSet.of(BackEnd.DataRow, BackEnd.CommandComplete);
    
    public class DataRowIterator implements java.util.Iterator<DataRow> {

        final private RowDescription rowDescription;
        private CommandComplete commandComplete = null;
        private boolean finished = false;
        
        public DataRowIterator(final RowDescription rowDescription) {
            this.rowDescription = rowDescription;
        }

        public boolean hasNext() {
            if(finished == true) {
                return false;
            }
            
            final BackEnd nextMessage = BackEnd.find(stream.peek());
            if(nextMessage == BackEnd.CommandComplete) {
                finished = true;
                commandComplete = (CommandComplete) stream.next(DRI_WILL_HANDLE);
                return false;
            }
            else if(nextMessage == BackEnd.DataRow) {
                return true;
            }
            else {
                throw new IllegalStateException("Wasn't expecting message of type " + nextMessage);
            }
        }

        public DataRow next() {
            if(finished) {
                throw new NoSuchElementException();
            }
            
            return (DataRow) stream.next(DRI_WILL_HANDLE);
        }
    }
}
