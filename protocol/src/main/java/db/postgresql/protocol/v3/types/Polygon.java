package db.postgresql.protocol.v3.types;

import java.util.List;
import db.postgresql.protocol.v3.serializers.UdtInput;

public class Polygon extends Path {

    public String getName() { return "polygon"; }
    
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
