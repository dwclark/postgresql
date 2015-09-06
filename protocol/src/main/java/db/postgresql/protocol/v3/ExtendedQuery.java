package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.serializers.*;
import java.util.List;
import db.postgresql.protocol.v3.typeinfo.Registry;

public class ExtendedQuery extends Query {

    public ExtendedQuery(final String query, final Session session) {
        super(session);
        prepare(query);
    }

    public ExtendedQuery execute(final List<Bindable[]> arguments) {
        for(Bindable[] ary : arguments) {
            session.bind("", "", ary, Session.EMPTY_FORMATS);
            session.describePortal("");
            session.execute("");
        }

        session.sync();
        return this;
    }

    private void prepare(final String query) {
        session.parse("", query, Session.EMPTY_OIDS);
    }

    //binders
    public <T> Bindable bind(final T val) {
        @SuppressWarnings("unchecked")
        Class<T> type = (Class<T>) val.getClass();
        return Registry.serializer(session.getDatabase(), type).bindable(val);
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
