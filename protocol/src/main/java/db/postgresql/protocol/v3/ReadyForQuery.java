package db.postgresql.protocol.v3;

public class ReadyForQuery extends Response {

    private final TransactionStatus status;
    
    public TransactionStatus getStatus() {
        return status;
    }

    private ReadyForQuery(final TransactionStatus status) {
        super(BackEnd.ReadyForQuery);
        this.status = status;
    }
    
    public static final ResponseBuilder builder = new ResponseBuilder() {
            public ReadyForQuery build(final BackEnd backEnd, final int size, final Session session) {
                return new ReadyForQuery(TransactionStatus.from(session.get()));
            }
        };
}

