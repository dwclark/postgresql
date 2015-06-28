package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;

public class ReadyForQuery extends Response {

    public TransactionStatus getStatus() {
        return TransactionStatus.from(buffer.get(0));
    }

    private ReadyForQuery() {
        super(BackEnd.ReadyForQuery);
    }
    
    private ReadyForQuery(ReadyForQuery toCopy) {
        super(BackEnd.ReadyForQuery, toCopy);
    }

    @Override
    public ReadyForQuery copy() {
        return new ReadyForQuery(this);
    }

    private static final ThreadLocal<ReadyForQuery> tlData = new ThreadLocal<ReadyForQuery>() {
            @Override protected ReadyForQuery initialValue() {
                return new ReadyForQuery();
            }
        }
    
    public static final ResponseBuilder builder = new ResponseBuilder() {
            public ReadyForQuery build(final BackEnd backEnd, final int size, final Stream stream) {
                return (ReadForQuery) tlData.get().reset(stream.getRecord(size), stream.getEncoding());
            }
        };
}

