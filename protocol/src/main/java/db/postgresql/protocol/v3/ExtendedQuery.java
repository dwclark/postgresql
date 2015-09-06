package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.serializers.*;
import java.util.List;


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
    public <T> Bindable bind(final T val) {
        @SuppressWarnings("unchecked")
        Class<T> type = (Class<T>) val.getClass();
        return stream.serializer(type).bindable(val);
    }
    
    public Bindable bind(final boolean val) {
        return BooleanSerializer.instance.bindable(val);
    }

    public Bindable bind(final double val) {
        return DoubleSerializer.instance.bindable(val);
    }

    public Bindable bind(final float val) {
        return FloatSerializer.instance.bindable(val);
    }

    public Bindable bind(final int val) {
        return IntSerializer.instance.bindable(val);
    }

    public Bindable bind(final long val) {
        return LongSerializer.instance.bindable(val);
    }

    public Bindable bind(final short val) {
        return ShortSerializer.instance.bindable(val);
    }
}
