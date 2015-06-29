package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;

public class KeyData extends Response {

    public int getPid() {
        return buffer.getInt(0);
    }

    public int getSecretKey() {
        return buffer.getInt(4);
    }
    
    private KeyData() {
        super(BackEnd.BackendKeyData);
    }

    private KeyData(KeyData toCopy) {
        super(toCopy);
    }

    @Override
    public KeyData copy() {
        return new KeyData(this);
    }

    private static final ThreadLocal<KeyData> tlData = new ThreadLocal<KeyData>() {
            @Override protected KeyData initialValue() {
                return new KeyData();
            }
        };
    
    public static final ResponseBuilder builder = new ResponseBuilder() {
            public KeyData build(final BackEnd backEnd, final int size, final Stream stream) {
                return (KeyData) tlData.get().reset(stream.getRecord(size), stream.getEncoding());
            }
        };
}
