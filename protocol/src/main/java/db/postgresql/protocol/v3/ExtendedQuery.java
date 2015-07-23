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
        return BooleanSerializer.instance.bindable(val, Format.TEXT);
    }

    public Bindable bind(final byte[] val) {
        return BytesSerializer.instance.bindable(val, Format.TEXT);
    }

    public Bindable bind(final LocalDate val) {
        return DateSerializer.instance.bindable(val, Format.TEXT);
    }

    public Bindable bind(final double val) {
        return DoubleSerializer.instance.bindable(val, Format.TEXT);
    }

    public Bindable bind(final float val) {
        return FloatSerializer.instance.bindable(val, Format.TEXT);
    }

    public Bindable bind(final int val) {
        return IntSerializer.instance.bindable(val, Format.TEXT);
    }

    public Bindable bind(final LocalDateTime val) {
        return LocalDateTimeSerializer.instance.bindable(val, Format.TEXT);
    }

    public Bindable bind(final LocalTime val) {
        return LocalTimeSerializer.instance.bindable(val, Format.TEXT);
    }

    public Bindable bind(final long val) {
        return LongSerializer.instance.bindable(val, Format.TEXT);
    }

    public Bindable bind(final Money val) {
        return stream.getMoneySerializer().bindable(val, Format.TEXT);
    }

    public Bindable bind(final BigDecimal val) {
        return stream.getNumericSerializer().bindable(val, Format.TEXT);
    }

    public Bindable bind(final OffsetDateTime val) {
        return OffsetDateTimeSerializer.instance.bindable(val, Format.TEXT);
    }

    public Bindable bind(final OffsetTime val) {
        return OffsetTimeSerializer.instance.bindable(val, Format.TEXT);
    }

    public Bindable bind(final short val) {
        return ShortSerializer.instance.bindable(val, Format.TEXT);
    }
    
    public Bindable bind(final String val) {
        return stream.getStringSerializer().bindable(val, Format.TEXT);
    }
}
