package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.typeinfo.Registry;
import db.postgresql.protocol.v3.Session;
import db.postgresql.protocol.v3.ProtocolException;
import db.postgresql.protocol.v3.types.Udt;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.function.BiFunction;

public class UdtParser implements UdtInput {

    final CompositeEngine engine;
    final Session session;
    
    private UdtParser(final Session session, final CharSequence buffer, final BiFunction<Character,Integer,? extends CompositeMeta> factory) {
        this.session = session;
        this.engine = new CompositeEngine<>(buffer, factory);
    }

    public static UdtParser forUdt(final Session session, final CharSequence buffer) {
        return new UdtParser(session, buffer, CompositeMeta.udt);
    }

    public static UdtParser forGeometry(final Session session, final CharSequence buffer) {
        return new UdtParser(session, buffer, CompositeMeta.geometry);
    }
    
    private static final Class[] CONSTRUCTOR_ARGS = new Class[] { UdtInput.class };
    
    public <T> T read(Class<T> type) {
        if(Udt.class.isAssignableFrom(type)) {
            return readUdt(type);
        }
        else {
            final String val = engine.getField();
            final Serializer<T> serializer = Registry.serializer(session.getDatabase(), type);
            if(val == null) {
                return null;
            }
            else {
                return serializer.fromString(val);
            }
        }
    }

    private <T> T readUdt(Class<T> type) {
        try {
            engine.beginUdt();
            Constructor<T> constructor = type.getConstructor(CONSTRUCTOR_ARGS);
            T udt = (T) constructor.newInstance(this);
            engine.endUdt();
            return udt;
        }
        catch(NoSuchMethodException | InstantiationException |
              IllegalAccessException | InvocationTargetException ex) {
            throw new ProtocolException(ex);
        }
    }

    public char getCurrentDelimiter() {
        return engine.getLevel().getBegin();
    }

    public boolean hasNext() {
        return engine.getCurrent() != engine.getLevel().getEnd();
    }
}
