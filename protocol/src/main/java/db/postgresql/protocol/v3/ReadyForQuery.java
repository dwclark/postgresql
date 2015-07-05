package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;

public class ReadyForQuery extends Response {

    private final TransactionStatus status;
    
    public TransactionStatus getStatus() {
        return status;
    }

    private ReadyForQuery(final Stream stream) {
        super(BackEnd.ReadyForQuery);
        this.status = TransactionStatus.from(stream.get());
    }
    
    public static final ResponseBuilder builder = new ResponseBuilder() {
            public ReadyForQuery build(final BackEnd backEnd, final int size, final Stream stream) {
                return new ReadyForQuery(stream);
            }
        };
}

