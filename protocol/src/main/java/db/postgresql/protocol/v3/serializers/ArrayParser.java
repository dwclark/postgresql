package db.postgresql.protocol.v3.serializers;

import java.util.function.BiFunction;
import java.util.Collection;
import java.util.Deque;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.lang.reflect.Array;

public class ArrayParser {

    private final CompositeEngine engine;
    
    
    class IndexTrackingDeque implements Deque<CompositeMeta.Array> {

        final int[] dimensions;
        
        public IndexTrackingDeque(final CharSequence buffer) {
            int count = 0;
            while(permLevel.isBegin(buffer.charAt(count))) {
                ++count;
            }

            dimensions = new int[count];
        }
        
        private final ArrayDeque<CompositeMeta.Array> impl = new ArrayDeque<>();
        
        public void addFirst(CompositeMeta.Array arrayMeta) {
            throw new UnsupportedOperationException();
        }

        public void addLast(CompositeMeta.Array arrayMeta) {
            throw new UnsupportedOperationException();
        }

        public boolean offerFirst(CompositeMeta.Array arrayMeta) {
            throw new UnsupportedOperationException();
        }

        public boolean offerLast(CompositeMeta.Array arrayMeta) {
            throw new UnsupportedOperationException();
        }

        public CompositeMeta.Array removeFirst() {
            throw new UnsupportedOperationException();
        }

        public CompositeMeta.Array removeLast() {
            throw new UnsupportedOperationException();
        }

        public CompositeMeta.Array pollFirst() {
            throw new UnsupportedOperationException();
        }

        public CompositeMeta.Array pollLast() {
            throw new UnsupportedOperationException();
        }

        public CompositeMeta.Array getFirst() {
            throw new UnsupportedOperationException();
        }

        public CompositeMeta.Array getLast() {
            throw new UnsupportedOperationException();
        }

        public CompositeMeta.Array peekFirst() {
            throw new UnsupportedOperationException();
        }

        public CompositeMeta.Array peekLast() {
            throw new UnsupportedOperationException();
        }

        public boolean removeFirstOccurrence(Object o) {
            throw new UnsupportedOperationException();
        }

        public boolean removeLastOccurrence(Object o) {
            throw new UnsupportedOperationException();
        }

        public boolean add(CompositeMeta.Array arrayMeta) {
            throw new UnsupportedOperationException();
        }

        public boolean offer(CompositeMeta.Array arrayMeta) {
            throw new UnsupportedOperationException();
        }

        public CompositeMeta.Array remove() {
            throw new UnsupportedOperationException();
        }

        public CompositeMeta.Array poll() {
            throw new UnsupportedOperationException();
        }

        public CompositeMeta.Array element() {
            throw new UnsupportedOperationException();
        }

        public CompositeMeta.Array peek() {
            return impl.peek();
        }

        public void push(CompositeMeta.Array arrayMeta) {
            impl.push(arrayMeta);
        }

        public CompositeMeta.Array pop() {
            return impl.pop();
        }

        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        public boolean containsAll(Collection<?> collection) {
            throw new UnsupportedOperationException();
        }

        public boolean addAll(Collection<? extends CompositeMeta.Array> collection) {
            throw new UnsupportedOperationException();
        }

        public boolean removeAll(Collection<?> collection) {
            throw new UnsupportedOperationException();
        }

        public boolean retainAll(Collection<?> collection) {
            throw new UnsupportedOperationException();
        }

        public void clear() {
            impl.clear();
        }

        public boolean contains(Object o) {
            throw new UnsupportedOperationException();
        }

        public int size() {
            return impl.size();
        }

        public boolean isEmpty() {
            return impl.isEmpty();
        }

        public Iterator<CompositeMeta.Array> iterator() {
            throw new UnsupportedOperationException();
        }

        public Object[] toArray() {
            throw new UnsupportedOperationException();
        }

        public <T> T[] toArray(T[] ts) {
            throw new UnsupportedOperationException();
        }

        public Iterator<CompositeMeta.Array> descendingIterator() {
            throw new UnsupportedOperationException();
        }
    }

    private final CompositeMeta.Array permLevel;
    private final IndexTrackingDeque deque;
    private final Serializer serializer;
    
    public ArrayParser(final CharSequence buffer, final Serializer serializer, final char delimiter) {
        this.permLevel = new CompositeMeta.Array(0, delimiter);
        this.deque = new IndexTrackingDeque(buffer);
        this.engine = new CompositeEngine<CompositeMeta.Array>(buffer, CompositeMeta.array(delimiter), deque);
        this.serializer = serializer;
        parseDimensions();
    }

    private final void parseDimensions() {
        engine.beginUdt();
        while(true) {
            char c = engine.getCurrent();
            if(permLevel.isBegin(c)) {
                ++deque.dimensions[engine.getLevel().getLevel()];
                engine.beginUdt();
                for(int i = engine.getLevel().getLevel(); i < deque.dimensions.length; ++i) {
                    deque.dimensions[i] = 0;
                }
                continue;
            }

            if(permLevel.isEnd(c)) {
                if(deque.size() == 1) {
                    engine.endUdt();
                    break;
                }
                else {
                    engine.endUdt();
                    continue;
                }
            }

            engine.findBoundaries();
            ++deque.dimensions[engine.getLevel().getLevel()];
        }
    }

    public int[] getDimensions() {
        return deque.dimensions;
    }

    public int getNumElements() {
        int total = 1;
        for(int i = 0; i < deque.dimensions.length; ++i) {
            total *= deque.dimensions[i];
        }

        return total;
    }

    public Object toArray() {
        engine.reset();
        final Object theArray = Array.newInstance(serializer.getArrayType(), deque.dimensions);
        final int target = getNumElements();
        int total = 0;
        final int[] dimensions = new int[deque.dimensions.length];

        while(total != target) {
            char c = engine.getCurrent();
            if(permLevel.isBegin(c)) {
                engine.beginUdt();
            }
            else if(permLevel.isEnd(c)) {
                engine.endUdt();
            }
            else {
                final Object ary = findArray(theArray, dimensions);
                serializer.putArray(ary, dimensions[dimensions.length - 1], engine.getField());
                dimensionIncrement(dimensions, dimensions.length - 1);
                ++total;
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

    public void dimensionIncrement(final int[] dimensions, int index) {
        final int current = dimensions[index];
        final int target = deque.dimensions[index];
        if(current < target) {
            ++dimensions[index];
            for(int i = index; i < dimensions.length; ++i) {
                dimensions[i] = 0;
            }
        }
        else {
            dimensionIncrement(dimensions, index - 1);
        }
    }
}
