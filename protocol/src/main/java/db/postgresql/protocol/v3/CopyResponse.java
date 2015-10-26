package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.PostgresqlStream;

public class CopyResponse extends Response {

    private final Format format;

    public Format getFormat() {
        return format;
    }

    private final Format[] columnFormats;

    public Format[] getColumnFormats() {
        return columnFormats;
    }
    
    public CopyResponse(final BackEnd backEnd, final PostgresqlStream stream, final int size) {
        super(backEnd, size);
        format = Format.from(stream.get() & 0xFF);
        int length = stream.getShort() & 0xFFFF;
        if(length > 0) {
            columnFormats = new Format[length];
            for(int i = 0; i < length; ++i) {
                columnFormats[i] = Format.from(stream.getShort() & 0xFFFF);
            }
        }
        else {
            columnFormats = Format.EMPTY_FORMATS;
        }
    }
}
