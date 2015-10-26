package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.PostgresqlStream;

public class ReadyForQuery extends Response {

    private final TransactionStatus status;
    
    public TransactionStatus getStatus() {
        return status;
    }

    public ReadyForQuery(final PostgresqlStream stream) {
        super(BackEnd.ReadyForQuery, 1);
        this.status = TransactionStatus.from(stream.get());
    }
}

