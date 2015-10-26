package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.PostgresqlStream;

public class KeyData extends Response {

    private final int pid;
    private final int secretKey;
    
    public int getPid() {
        return pid;
    }

    public int getSecretKey() {
        return secretKey;
    }
    
    public KeyData(final PostgresqlStream stream) {
        super(BackEnd.BackendKeyData, 8);
        this.pid = stream.getInt();
        this.secretKey = stream.getInt();
    }
}
