package db.postgresql.protocol.v3.serializers;

import java.util.function.BiFunction;
import java.util.Collection;
import java.util.Iterator;
import java.lang.reflect.Array;

public class ArrayParser {

    private final CompositeEngine engine;
    private final CompositeMeta.Array permLevel;
    private final Serializer serializer;
    private final int[] dimensions;
    private final int[] mods;

    private static class ArrayEngine extends CompositeEngine<CompositeMeta.Array> {
        public ArrayEngine(final CharSequence buffer, final char delimiter) {
            super(buffer, CompositeMeta.array(delimiter));
        }

        @Override
        public String getField() {
            String tmp = super.getField();
            if(!getWithinQuotes() && tmp.equalsIgnoreCase("null")) {
                return null;
            }
            else {
                return tmp;
            }
        }
    }
    
    public ArrayParser(final CharSequence buffer, final Serializer serializer, final char delimiter) {
        this.permLevel = new CompositeMeta.Array(0, delimiter);
        this.engine = new ArrayEngine(buffer, delimiter);
        this.serializer = serializer;
        this.dimensions = allocateDimensions(permLevel, buffer);
        parseDimensions(permLevel, engine, dimensions);
        this.mods = mods(dimensions);
    }

    public static int[] allocateDimensions(final CompositeMeta.Array permLevel, final CharSequence buffer) {
        int count = 0;
        while(permLevel.isBegin(buffer.charAt(count))) {
            ++count;
        }
        
        return new int[count];
    }

    public static void parseDimensions(final CompositeMeta.Array permLevel, final CompositeEngine engine,
                                       final int[] dimensions) {
        engine.beginUdt();
        while(true) {
            char c = engine.getCurrent();
            if(permLevel.isBegin(c)) {
                ++dimensions[engine.getLevel().getLevel()];
                engine.beginUdt();
                for(int i = engine.getLevel().getLevel(); i < dimensions.length; ++i) {
                    dimensions[i] = 0;
                }
                continue;
            }

            if(permLevel.isEnd(c)) {
                if(engine.levels.size() == 1) {
                    engine.endUdt();
                    break;
                }
                else {
                    engine.endUdt();
                    continue;
                }
            }

            engine.findBoundaries();
            ++dimensions[engine.getLevel().getLevel()];
        }
    }

    public static int[] mods(final int[] dimensions) {
        final int[] ret = new int[dimensions.length];
        for(int i = dimensions.length - 1; i >= 0; --i) {
            if(i == dimensions.length - 1) {
                ret[i] = dimensions[i];
            }
            else {
                ret[i] = dimensions[i] * ret[i+1];
            }
        }

        return ret;
    }

    public static void calculateIndexes(final int[] mods, final int[] indexes, final int location) {
        for(int i = 0; i < indexes.length; ++i) {
            indexes[i] = 0;
        }

        int left = location;
        
        for(int i = 0; i < mods.length - 1; ++i) {
            final int multiplier = left / mods[i+1];
            indexes[i] = (multiplier > 0) ? multiplier : 0;
            left -= multiplier * mods[i+1];
        }

        indexes[mods.length - 1] = left;
    }

    public int[] getDimensions() {
        return dimensions;
    }

    public int getNumElements() {
        int total = 1;
        for(int i = 0; i < dimensions.length; ++i) {
            total *= dimensions[i];
        }

        return total;
    }

    public Object toArray() {
        engine.reset();
        final Object theArray = Array.newInstance(serializer.getArrayType(), dimensions);
        final int numElements = getNumElements();
        int index = 0;
        final int[] indexes = new int[dimensions.length];

        while(index < numElements) {
            char c = engine.getCurrent();
            if(permLevel.isBegin(c)) {
                engine.beginUdt();
            }
            else if(permLevel.isEnd(c)) {
                engine.endUdt();
            }
            else {
                calculateIndexes(mods, indexes, index);
                final Object ary = findArray(theArray, indexes);
                serializer.putArray(ary, indexes[indexes.length - 1], engine.getField());
                ++index;
            }
        }

        return theArray;
    }

    private static Object findArray(final Object theArray, final int[] dimensions) {
        Object ret = theArray;
        for(int i = 0; i < dimensions.length - 1; ++i) {
            ret = Array.get(ret, dimensions[i]);
        }

        return ret;
    }
}
