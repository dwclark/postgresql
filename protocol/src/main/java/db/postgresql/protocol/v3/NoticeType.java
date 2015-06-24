package db.postgresql.protocol.v3;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public enum NoticeType {
    Severity('S'),
    Code('C'),
    Message('M'),
    Detail('D'),
    Hint('H'),
    Position('P'),
    InternalPosition('p'),
    InternalQuery('q'),
    Where('W'),
    SchemaName('s'),
    TableName('t'),
    ColumnName('c'),
    DataTypeName('d'),
    ConstraintName('n'),
    File('F'),
    Line('L'),
    Routine('R'),
    Unknown(' ');

    private NoticeType(char c) {
        this.code = (byte) c;
    }

    public final byte code;

    private static final Map<Byte,NoticeType> map;

    static {
        Map<Byte,NoticeType> tmp = new LinkedHashMap<>();
        for(NoticeType nt : values()) {
            tmp.put(nt.code, nt);
        }

        map = Collections.unmodifiableMap(tmp);
    }
    
    public static NoticeType from(byte b) {
        if(map.containsKey(b)) {
            return map.get(b);
        }
        else {
            return NoticeType.Unknown;
        }
    }
}
