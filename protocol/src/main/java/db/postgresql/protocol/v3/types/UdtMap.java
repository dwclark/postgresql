package db.postgresql.protocol.v3.types;

import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import db.postgresql.protocol.v3.typeinfo.PgType;
import db.postgresql.protocol.v3.serializers.UdtOutput;

public class UdtMap extends AbstractMap<String,Object> implements Udt {

    final private List<Map.Entry<String,Object>> entries;
    final String name;
    
    private class EntrySet extends AbstractSet<Map.Entry<String,Object>> {
        
        @Override
        public int size() {
            return entries.size();
        }

        @Override
        public Iterator<Map.Entry<String,Object>> iterator() {
            return entries.iterator();
        }
    }

    public Set<Map.Entry<String,Object>> entrySet() {
        return new EntrySet();
    }

    @Override
    public int size() {
        return entries.size();
    }

    public UdtMap(final List<Map.Entry<String,Object>> entries, final PgType pgType) {
        this.entries = Collections.unmodifiableList(entries);
        this.name = pgType.getFullName();
    }

    public String getName() {
        return name;
    }

    public void write(UdtOutput output) {
        throw new UnsupportedOperationException();
    }

    public static Map.Entry<String,Object> entry(final String key, final Object value) {
        return new SimpleImmutableEntry<>(key, value);
    }
}
