package db.postgresql.protocol.v3.types;

import java.util.List;
import db.postgresql.protocol.v3.serializers.UdtInput;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class Polygon extends Path {

    public static final PgType PGTYPE =
        new PgType.Builder().name("polygon").oid(604).arrayId(1027).build();
    
    public String getName() { return PGTYPE.getName(); }
    
    public Polygon(final UdtInput input) {
        super(input);
    }

    public Polygon(final List<Point> points) {
        super(points, false);
    }

    @Override
    public boolean equals(Object o) {
        if(o == null) {
            return false;
        }
        
        if(!o.getClass().equals(Polygon.class)) {
            return false;
        }
        
        Polygon rhs = (Polygon) o;
        return pointsEqual(rhs.getPoints());
    }
}
