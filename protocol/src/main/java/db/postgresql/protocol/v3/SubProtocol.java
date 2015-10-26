package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.PostgresqlStream;
import db.postgresql.protocol.v3.io.NoData;
import java.util.function.Predicate;

public abstract class SubProtocol {

    public void drive(final PostgresqlStream stream) {
        final ResponseBuilder builder = getResponseBuilder();
        final Map<BackEnd,BiPredicate<PostgresqlStream,Response>> handlers = getHandlers();
        
        boolean keepGoing = start(stream);

        while(keepGoing) {
            try {
                BackEnd backEnd = BackEnd.find(stream.get(1));
                Response r = builders.build(backEnd, stream, stream.getInt() - 4);
                keepGoing = handlers.get(backEnd).test(stream, r);
            }
            catch(NoData noData) {
                keepGoing = whenNoData();
            }
            catch(Exception e) {

            }
        }

        cleanup();
    }

    public boolean start(final PostgresqlStream stream) {
        return true;
    }

    public void cleanup() {
        // do nothing
    }

    public boolean whenNoData() {
        return true;
    }

    public abstract ResponseBuilder getResponseBuilder();
    public abstract Map<BackEnd,BiPredicate<PostgresqlStream,Response>> getHandlers();
}
