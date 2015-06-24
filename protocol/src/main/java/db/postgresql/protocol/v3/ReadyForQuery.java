package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;

public class ReadyForQuery extends Response {

    private final TransactionStatus status;

    public TransactionStatus getStatus() {
        return status;
    }

    public ReadyForQuery(final BackEnd backEnd, final TransactionStatus status) {
        super(backEnd);
        this.status = status;
    }
    
    public final ResponseBuilder readyForQueryBuilder = new ResponseBuilder() {
            public ReadyForQuery build(final BackEnd backEnd, final int size, final Stream stream) {
                return new ReadyForQuery(backEnd, TransactionStatus.from(stream.get()));
            }
        };
}

