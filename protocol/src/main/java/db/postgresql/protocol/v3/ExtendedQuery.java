package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.serializers.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

public class ExtendedQuery implements ResultProvider {

    private final Session stream;
    private Response response;
    
    private static final EnumSet<BackEnd> WILL_HANDLE =
        EnumSet.of(BackEnd.RowDescription, BackEnd.EmptyQueryResponse, BackEnd.ReadyForQuery,
                   BackEnd.CommandComplete, BackEnd.DataRow);

    public ExtendedQuery(final String query, final Session stream) {
        this.stream = stream;
        prepare(query);
    }

    public void execute(final List<Bindable[]> arguments) {
        for(Bindable[] ary : arguments) {
            stream.bind("", "", ary, Session.EMPTY_FORMATS).execute("");
        }

        stream.sync();
    }

    private void prepare(final String query) {
        stream.parse("", query, Session.EMPTY_OIDS);
        
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

    //binders
    public Bindable bind(final boolean val) {
        return stream.serializer(BooleanSerializer.class).bindable(val, Format.TEXT);
    }

    public Bindable bind(final byte[] val) {
        return stream.serializer(BytesSerializer.class).bindable(val, Format.TEXT);
    }

    public Bindable bind(final LocalDate val) {
        return stream.serializer(DateSerializer.class).bindable(val, Format.TEXT);
    }

    public Bindable bind(final double val) {
        return stream.serializer(DoubleSerializer.class).bindable(val, Format.TEXT);
    }

    public Bindable bind(final float val) {
        return stream.serializer(FloatSerializer.class).bindable(val, Format.TEXT);
    }

    public Bindable bind(final int val) {
        return stream.serializer(IntSerializer.class).bindable(val, Format.TEXT);
    }

    public Bindable bind(final LocalDateTime val) {
        return stream.serializer(LocalDateTimeSerializer.class).bindable(val, Format.TEXT);
    }

    public Bindable bind(final LocalTime val) {
        return stream.serializer(LocalTimeSerializer.class).bindable(val, Format.TEXT);
    }

    public Bindable bind(final long val) {
        return stream.serializer(LongSerializer.class).bindable(val, Format.TEXT);
    }

    public Bindable bind(final Money val) {
        return stream.serializer(MoneySerializer.class).bindable(val, Format.TEXT);
    }

    public Bindable bind(final BigDecimal val) {
        return stream.serializer(NumericSerializer.class).bindable(val, Format.TEXT);
    }

    public Bindable bind(final OffsetDateTime val) {
        return stream.serializer(OffsetDateTimeSerializer.class).bindable(val, Format.TEXT);
    }

    public Bindable bind(final OffsetTime val) {
        return stream.serializer(OffsetTimeSerializer.class).bindable(val, Format.TEXT);
    }

    public Bindable bind(final short val) {
        return stream.serializer(ShortSerializer.class).bindable(val, Format.TEXT);
    }
    
    public Bindable bind(final String val) {
        return stream.serializer(StringSerializer.class).bindable(val, Format.TEXT);
    }
}
