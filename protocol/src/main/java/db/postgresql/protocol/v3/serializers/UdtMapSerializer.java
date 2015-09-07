package db.postgresql.protocol.v3.serializers;

import java.util.SortedSet;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import db.postgresql.protocol.v3.Session;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.types.UdtMap;
import db.postgresql.protocol.v3.typeinfo.PgType;
import db.postgresql.protocol.v3.typeinfo.Registry;
import db.postgresql.protocol.v3.typeinfo.PgAttribute;

public class UdtMapSerializer extends Serializer<UdtMap> {

    private final Session session;
    private final PgType pgType;
    
    public UdtMapSerializer(final Session session, final PgType pgType) {
        super(UdtMap.class);
        this.session = session;
        this.pgType = pgType;
    }

    public UdtMap fromString(final String str) {
        final UdtParser parser = UdtParser.forUdt(session, str);
        final SortedSet<PgAttribute> attrs = pgType.getAttributes();
        final List<Map.Entry<String,Object>> entries = new ArrayList<>(attrs.size());

        parser.engine.beginUdt();
        for(PgAttribute attr : attrs) {
            final PgType attrPgType = attr.pgType(session);
            final Serializer serializer = Registry.serializer(session, attrPgType);
            final String field = parser.engine.getField();
            if(field == null) {
                entries.add(UdtMap.entry(attr.getName(), null));
            }
            else {
                entries.add(UdtMap.entry(attr.getName(), serializer.fromString(field)));
            }
        }
        parser.engine.endUdt();

        return new UdtMap(entries, pgType);
    }
    
    public UdtMap read(final Stream stream, final int size) {
        return isNull(size) ? null : fromString(str(stream, size));
    }

    public int length(final UdtMap val) {
        throw new UnsupportedOperationException();
    }

    public void write(final Stream stream, final UdtMap val) {
        throw new UnsupportedOperationException();
    }
}
