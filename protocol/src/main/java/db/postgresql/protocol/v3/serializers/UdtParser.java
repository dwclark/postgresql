package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.ProtocolException;
import db.postgresql.protocol.v3.io.Stream;
import java.util.Stack;
import java.nio.CharBuffer;
import java.math.BigInteger;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.function.BiFunction;

public class UdtParser implements UdtInput {

    final CompositeEngine engine;
    
    private UdtParser(final CharSequence buffer, final BiFunction<Character,Integer,ParserMeta> factory) {
        this.engine = new CompositeEngine(buffer, factory);
    }

    public static UdtParser forUdt(final CharSequence buffer) {
        return new UdtParser(buffer, ParserMeta.udt);
    }

    public static UdtParser forGeometry(final CharSequence buffer) {
        return new UdtParser(buffer, ParserMeta.geometry);
    }
    
    public Boolean readBoolean() {
        String s = engine.getField();
        if(s == null) {
            return null;
        }
        
        if(s.length() != 1) {
            throw new IllegalStateException("Field is too large to be a boolean");
        }
        
        char val = s.charAt(0);
        if(val == BooleanSerializer.T) {
            return true;
        }
        else if(val == BooleanSerializer.F) {
            return false;
        }
        else {
            throw new IllegalStateException(val + " is not a valid boolean value");
        }
    }

    public Short readShort() {
        String s = engine.getField();
        return (s == null) ? null : Short.valueOf(s);
    }

    public Integer readInteger() {
        String s = engine.getField();
        return (s == null) ? null : Integer.valueOf(s);
    }

    public Long readLong() {
        String s = engine.getField();
        return (s == null) ? null : Long.valueOf(s);
    }

    public Float readFloat() {
        String s = engine.getField();
        return (s == null) ? null : Float.valueOf(s);
    }

    public Double readDouble() {
        String s = engine.getField();
        return (s == null) ? null : Double.valueOf(s);
    }

    public String readString() {
        return engine.getField();
    }

    private static final Class[] CONSTRUCTOR_ARGS = new Class[] { UdtInput.class };
    
    public <T extends Udt> T readUdt(Class<T> type) {
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
        return engine.getLevel().getDelimiter();
    }

    public boolean hasNext() {
        return engine.getCurrent() != engine.getLevel().getEndDelimiter();
    }
}
