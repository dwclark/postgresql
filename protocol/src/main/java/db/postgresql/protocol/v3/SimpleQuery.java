package db.postgresql.protocol.v3;

import java.util.Iterator;
import java.util.List;
import java.util.EnumSet;

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
}
