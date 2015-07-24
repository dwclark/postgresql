package db.postgresql.protocol.v3;

import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.EnumSet;
import java.util.function.Function;

public class SimpleQuery implements ResultProvider {

    private final Session stream;
    private Response response;
    
    private static final EnumSet<BackEnd> WILL_HANDLE =
        EnumSet.of(BackEnd.RowDescription, BackEnd.EmptyQueryResponse, BackEnd.ReadyForQuery,
                   BackEnd.CommandComplete, BackEnd.DataRow);

    public SimpleQuery(final String query, final Session stream) {
        this.stream = stream;
        stream.query(query);
    }

    public TransactionStatus getStatus() {
        if(response.getBackEnd() == BackEnd.ReadyForQuery) {
            return ((ReadyForQuery) response).getStatus();
        }
        else {
            return null;
        }
    }

    public boolean isDone() {
        return (response instanceof ReadyForQuery);
    }
    
    public void advance() {
        if(!isDone()) {
            response = stream.next(WILL_HANDLE);
        }
    }

    public Response getResponse() {
        return response;
    }

    public Results nextResults() {
        advance();
        return Results.nextResults(this);
    }

    public <R> List<R> manyRows(final Function<DataRow, R> func) {
        List<R> ret = new ArrayList<>();
        Results results;
        while(null != (results = nextResults())) {
            if(results.getResultType() == ResultType.HAS_RESULTS) {
                Iterator<DataRow> rowIter = results.rows();
                while(rowIter.hasNext()) {
                    ret.add(func.apply(rowIter.next()));
                }
            }
        }

        return ret;
    }

    public <R> R singleRow(final Function <DataRow,R> func) {
        List<R> many = manyRows(func);
        if(many.size() != 1) {
            throw new IllegalStateException("Single result expected, however query returned: " +
                                            many.size() + " results");
        }

        return many.get(0);
    }

    public <R> List<R> manyResults(final Function<DataRow.Iterator, R> func) {
        return manyRows((DataRow dataRow) -> func.apply(dataRow.iterator()));
    }

    public <R> R singleResult(final Function<DataRow.Iterator, R> func) {
        return singleRow((DataRow dataRow) -> func.apply(dataRow.iterator()));
    }
}
