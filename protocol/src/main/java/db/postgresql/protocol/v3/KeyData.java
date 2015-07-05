package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;

public class KeyData extends Response {

    private final int pid;
    private final int secretKey;
    
    public int getPid() {
        return pid;
    }

    public int getSecretKey() {
        return secretKey;
    }
    
    private KeyData(final Stream stream) {
        super(BackEnd.BackendKeyData);
        this.pid = stream.getInt();
        this.secretKey = stream.getInt();
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public KeyData build(final BackEnd backEnd, final int size, final Stream stream) {
                return new KeyData(stream);
            }
        };
}
