package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.serializers.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

public class ExtendedQuery extends Query {

    public ExtendedQuery(final String query, final Session stream) {
        super(stream);
        prepare(query);
    }

    public ExtendedQuery execute(final List<Bindable[]> arguments) {
        for(Bindable[] ary : arguments) {
            stream.bind("", "", ary, Session.EMPTY_FORMATS);
            stream.describePortal("");
            stream.execute("");
        }

        stream.sync();
        return this;
    }

    private void prepare(final String query) {
        stream.parse("", query, Session.EMPTY_OIDS);
    }

    //binders
    public Bindable bind(final BitSet val) {
        return BitSetSerializer.instance.bindable(val, Format.TEXT);
    }
    
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

    public Bindable bind(final UUID val) {
        return UUIDSerializer.instance.bindable(val, Format.TEXT);
    }
}
