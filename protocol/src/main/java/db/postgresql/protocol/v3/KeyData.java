package db.postgresql.protocol.v3;

public class KeyData extends Response {

    private final int pid;
    private final int secretKey;
    
    public int getPid() {
        return pid;
    }

    public int getSecretKey() {
        return secretKey;
    }
    
    private KeyData(final PostgresqlStream stream) {
        super(BackEnd.BackendKeyData);
        this.pid = stream.getInt();
        this.secretKey = stream.getInt();
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public KeyData build(final BackEnd backEnd, final int size, final PostgresqlStream stream) {
                return new KeyData(stream);
            }
        };
}
