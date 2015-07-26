package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.util.List;
import java.util.Collections;

public interface Bindable {
    public static final Bindable[] ZERO_ELEMENTS = new Bindable[0];
    public static final List<Bindable[]> EMPTY = Collections.singletonList(ZERO_ELEMENTS);
    
    Format getFormat();
    int getLength();
    void write(Stream stream);
}
